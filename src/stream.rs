use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;

use crate::codec::{arg, Decode};
use crate::krpc::schema as proto;
use crate::{ClientRef, Error, Result};

/// Latest raw value for a stream: the encoded value, or the per-update
/// error reported by the server.
pub(crate) type StreamValue = std::result::Result<Vec<u8>, proto::Error>;

type ValueSender = watch::Sender<Option<StreamValue>>;
type ValueReceiver = watch::Receiver<Option<StreamValue>>;

struct Entry {
    tx: ValueSender,
    // The server deduplicates identical AddStream calls and returns the same
    // stream id, so several `Stream` handles can share one entry. The server
    // stream is only removed when the last handle goes away.
    refs: usize,
}

/// Routes `StreamUpdate` results from the stream connection to the watch
/// channel of each live `Stream` handle.
#[derive(Default)]
pub(crate) struct StreamRegistry {
    entries: Mutex<HashMap<u64, Entry>>,
}

impl StreamRegistry {
    /// Subscribes to stream `id`, creating the channel on first use.
    pub(crate) fn register(&self, id: u64) -> ValueReceiver {
        let mut entries = self.entries.lock().unwrap();
        match entries.get_mut(&id) {
            Some(entry) => {
                entry.refs += 1;
                entry.tx.subscribe()
            }
            None => {
                let (tx, rx) = watch::channel(None);
                entries.insert(id, Entry { tx, refs: 1 });
                rx
            }
        }
    }

    /// Drops one subscription to stream `id`. Returns true if it was the
    /// last one, in which case the caller should remove the server stream.
    pub(crate) fn release(&self, id: u64) -> bool {
        let mut entries = self.entries.lock().unwrap();
        match entries.get_mut(&id) {
            Some(entry) if entry.refs > 1 => {
                entry.refs -= 1;
                false
            }
            Some(_) => {
                entries.remove(&id);
                true
            }
            None => false,
        }
    }

    pub(crate) fn dispatch(&self, update: proto::StreamUpdate) {
        let entries = self.entries.lock().unwrap();
        for result in update.results {
            if let Some(entry) = entries.get(&result.id) {
                let pr = result.result.unwrap_or_default();
                let value = match pr.error {
                    Some(e) => Err(e),
                    None => Ok(pr.value),
                };
                let _ = entry.tx.send(Some(value));
            }
        }
    }

    /// Closes every channel; consumers observe `Error::Disconnected`.
    pub(crate) fn close(&self) {
        self.entries.lock().unwrap().clear();
    }
}

/// A handle to a server-side kRPC stream: the server repeatedly executes a
/// procedure and pushes the result, replacing polling with push updates.
///
/// The handle tracks the latest value. [`Stream::get`] returns it (waiting
/// only for the first update); [`Stream::next`] waits for a fresh update.
/// Dropping the handle removes the server stream once no other handle
/// refers to it.
///
/// `Stream` also implements [`futures_core::Stream`], yielding each update
/// as a `Result<T>`, so it works with `StreamExt` combinators:
///
/// ```ignore
/// use tokio_stream::StreamExt;
///
/// let mut altitude = flight.mean_altitude_stream().await?;
/// let samples: stayputnik::Result<Vec<f64>> = (&mut altitude).take(10).collect().await;
/// ```
///
/// The async iteration ends (yields `None`) when the stream connection
/// closes or the stream is removed. Note that the inherent
/// [`Stream::next`] shadows `StreamExt::next`; its semantics are the same,
/// minus the `Option` wrapper.
pub struct Stream<T> {
    id: u64,
    client: ClientRef,
    registry: Arc<StreamRegistry>,
    rx: ValueReceiver,
    /// Lazily-created pollable view for the `futures_core::Stream` impl.
    /// Built from a clone of `rx`, so it tracks its own "seen" cursor and
    /// does not interfere with [`Stream::get`]/[`Stream::next`].
    poller: Option<WatchStream<Option<StreamValue>>>,
    removed: bool,
    _marker: PhantomData<fn() -> T>,
}

impl<T: Decode> Stream<T> {
    pub(crate) fn new(
        id: u64,
        client: ClientRef,
        registry: Arc<StreamRegistry>,
        rx: ValueReceiver,
    ) -> Self {
        Self {
            id,
            client,
            registry,
            rx,
            poller: None,
            removed: false,
            _marker: PhantomData,
        }
    }

    /// The id the server assigned to this stream.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns the latest value, waiting only if no update has arrived yet.
    pub async fn get(&mut self) -> Result<T> {
        let client = self.client.clone();
        let guard = self
            .rx
            .wait_for(Option::is_some)
            .await
            .map_err(|_| Error::Disconnected)?;
        decode_value(&client, guard.as_ref().expect("checked by wait_for"))
    }

    /// Waits for the next update and returns its value.
    pub async fn next(&mut self) -> Result<T> {
        self.rx.changed().await.map_err(|_| Error::Disconnected)?;
        let client = self.client.clone();
        let guard = self.rx.borrow_and_update();
        decode_value(&client, guard.as_ref().expect("updates always carry a value"))
    }

    /// Sets the update rate in hertz. `0.0` updates every game tick.
    pub async fn set_rate(&self, hz: f32) -> Result<()> {
        self.client
            .invoke("KRPC", "SetStreamRate", &[arg(0, &self.id), arg(1, &hz)])
            .await?;
        Ok(())
    }

    /// Removes the stream from the server.
    ///
    /// This also happens automatically on drop; use this to observe errors.
    pub async fn remove(mut self) -> Result<()> {
        self.removed = true;
        if self.registry.release(self.id) {
            self.client
                .invoke("KRPC", "RemoveStream", &[arg(0, &self.id)])
                .await?;
        }
        Ok(())
    }
}

impl<T: Decode> futures_core::Stream for Stream<T> {
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let poller = this
            .poller
            .get_or_insert_with(|| WatchStream::from_changes(this.rx.clone()));
        loop {
            match Pin::new(&mut *poller).poll_next(cx) {
                Poll::Ready(Some(Some(value))) => {
                    return Poll::Ready(Some(decode_value(&this.client, &value)));
                }
                // Updates always carry a value; the initial `None`
                // placeholder is never re-sent. Skip defensively.
                Poll::Ready(Some(None)) => continue,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<T> Drop for Stream<T> {
    fn drop(&mut self) {
        if !self.removed && self.registry.release(self.id) {
            // Fire-and-forget: drop cannot await, but enqueueing on the
            // connection actor's channel is synchronous.
            self.client
                .invoke_forget("KRPC", "RemoveStream", vec![arg(0, &self.id)]);
        }
    }
}

fn decode_value<T: Decode>(client: &ClientRef, value: &StreamValue) -> Result<T> {
    match value {
        Ok(bytes) => T::decode_krpc(client, bytes),
        Err(e) => Err(Error::Procedure {
            service: e.service.clone(),
            name: e.name.clone(),
            description: e.description.clone(),
        }),
    }
}
