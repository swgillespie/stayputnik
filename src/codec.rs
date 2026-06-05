//! Conversion between Rust values and kRPC's wire encoding.
//!
//! kRPC encodes every procedure argument and return value as a standalone
//! protobuf-style value: varints for integers (zigzag for signed),
//! little-endian for floats, length-prefixed bytes for strings, and
//! protobuf messages for collections ([`List`](crate::krpc::schema::List),
//! [`Dictionary`](crate::krpc::schema::Dictionary), and friends). The
//! [`Encode`] and [`Decode`] traits implement that mapping; [`arg`] packs
//! an encoded value into a positioned procedure argument.
//!
//! [`Decode`] takes a [`ClientRef`] so that decoded remote-object handles
//! (and any collection containing them) capture the connection they came
//! from for future calls; scalar impls ignore it.
//!
//! This module is mainly the vocabulary of the generated service bindings
//! in [`services`](crate::services). User code rarely needs it directly,
//! but the traits are public: every generated class implements
//! `Encode`/`Decode` by its remote object id, and implementing them by
//! hand is only useful if you are issuing raw procedure calls.

use prost::Message;

use crate::krpc::schema as proto;
use crate::{ClientRef, Error, Result};

/// Encoding of a value as a kRPC procedure argument or return value.
pub trait Encode {
    fn encode_krpc(&self) -> Vec<u8>;
}

/// Decoding of a kRPC return value. Takes the client so that decoded class
/// instances (remote object handles) can hold a reference for future calls.
pub trait Decode: Sized {
    fn decode_krpc(client: &ClientRef, bytes: &[u8]) -> Result<Self>;
}

/// Builds a kRPC procedure argument from any encodable value.
pub fn arg(position: u32, value: &(impl Encode + ?Sized)) -> proto::Argument {
    proto::Argument {
        position,
        value: value.encode_krpc(),
    }
}

/// Builds a [`ProcedureCall`](crate::ProcedureCall) from a service name,
/// procedure name, and arguments.
pub fn call(service: &str, procedure: &str, arguments: Vec<proto::Argument>) -> proto::ProcedureCall {
    proto::ProcedureCall {
        service: service.to_string(),
        procedure: procedure.to_string(),
        arguments,
        ..Default::default()
    }
}

fn malformed(msg: impl Into<String>) -> Error {
    Error::Decode(prost::DecodeError::new(msg.into()))
}

// --- Varint helpers ---

fn encode_varint(value: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    prost::encoding::encode_varint(value, &mut buf);
    buf
}

fn decode_varint(bytes: &[u8]) -> Result<(u64, usize)> {
    let mut slice = bytes;
    let value = prost::encoding::decode_varint(&mut slice)?;
    let consumed = bytes.len() - slice.len();
    Ok((value, consumed))
}

fn zigzag_encode32(n: i32) -> u32 {
    ((n << 1) ^ (n >> 31)) as u32
}

fn zigzag_decode32(n: u32) -> i32 {
    ((n >> 1) as i32) ^ (-((n & 1) as i32))
}

fn zigzag_encode64(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

fn zigzag_decode64(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}

/// Decodes a length-prefixed byte region, bounds-checked.
fn decode_length_prefixed(bytes: &[u8]) -> Result<&[u8]> {
    let (len, offset) = decode_varint(bytes)?;
    let len = len as usize;
    bytes
        .get(offset..offset + len)
        .ok_or_else(|| malformed("length prefix exceeds buffer"))
}

// --- Primitive impls ---

impl Encode for u32 {
    fn encode_krpc(&self) -> Vec<u8> {
        encode_varint(*self as u64)
    }
}

impl Decode for u32 {
    fn decode_krpc(_client: &ClientRef, bytes: &[u8]) -> Result<Self> {
        let (v, _) = decode_varint(bytes)?;
        Ok(v as u32)
    }
}

impl Encode for u64 {
    fn encode_krpc(&self) -> Vec<u8> {
        encode_varint(*self)
    }
}

impl Decode for u64 {
    fn decode_krpc(_client: &ClientRef, bytes: &[u8]) -> Result<Self> {
        let (v, _) = decode_varint(bytes)?;
        Ok(v)
    }
}

impl Encode for i32 {
    fn encode_krpc(&self) -> Vec<u8> {
        encode_varint(zigzag_encode32(*self) as u64)
    }
}

impl Decode for i32 {
    fn decode_krpc(_client: &ClientRef, bytes: &[u8]) -> Result<Self> {
        let (v, _) = decode_varint(bytes)?;
        Ok(zigzag_decode32(v as u32))
    }
}

impl Encode for i64 {
    fn encode_krpc(&self) -> Vec<u8> {
        encode_varint(zigzag_encode64(*self))
    }
}

impl Decode for i64 {
    fn decode_krpc(_client: &ClientRef, bytes: &[u8]) -> Result<Self> {
        let (v, _) = decode_varint(bytes)?;
        Ok(zigzag_decode64(v))
    }
}

impl Encode for f32 {
    fn encode_krpc(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl Decode for f32 {
    fn decode_krpc(_client: &ClientRef, bytes: &[u8]) -> Result<Self> {
        let arr: [u8; 4] = bytes
            .try_into()
            .map_err(|_| malformed("expected 4 bytes for f32"))?;
        Ok(f32::from_le_bytes(arr))
    }
}

impl Encode for f64 {
    fn encode_krpc(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl Decode for f64 {
    fn decode_krpc(_client: &ClientRef, bytes: &[u8]) -> Result<Self> {
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| malformed("expected 8 bytes for f64"))?;
        Ok(f64::from_le_bytes(arr))
    }
}

impl Encode for bool {
    fn encode_krpc(&self) -> Vec<u8> {
        encode_varint(if *self { 1 } else { 0 })
    }
}

impl Decode for bool {
    fn decode_krpc(_client: &ClientRef, bytes: &[u8]) -> Result<Self> {
        let (v, _) = decode_varint(bytes)?;
        Ok(v != 0)
    }
}

impl Encode for str {
    fn encode_krpc(&self) -> Vec<u8> {
        let mut buf = encode_varint(self.len() as u64);
        buf.extend_from_slice(self.as_bytes());
        buf
    }
}

impl Encode for String {
    fn encode_krpc(&self) -> Vec<u8> {
        self.as_str().encode_krpc()
    }
}

impl Decode for String {
    fn decode_krpc(_client: &ClientRef, bytes: &[u8]) -> Result<Self> {
        let region = decode_length_prefixed(bytes)?;
        let s = std::str::from_utf8(region).map_err(|e| malformed(e.to_string()))?;
        Ok(s.to_string())
    }
}

impl<T: Encode + ?Sized> Encode for &T {
    fn encode_krpc(&self) -> Vec<u8> {
        (**self).encode_krpc()
    }
}

impl Encode for [u8] {
    fn encode_krpc(&self) -> Vec<u8> {
        let mut buf = encode_varint(self.len() as u64);
        buf.extend_from_slice(self);
        buf
    }
}

impl Encode for Vec<u8> {
    fn encode_krpc(&self) -> Vec<u8> {
        self.as_slice().encode_krpc()
    }
}

impl Decode for Vec<u8> {
    fn decode_krpc(_client: &ClientRef, bytes: &[u8]) -> Result<Self> {
        Ok(decode_length_prefixed(bytes)?.to_vec())
    }
}

// --- Collection impls ---

impl<T: Encode> Encode for Vec<T> {
    fn encode_krpc(&self) -> Vec<u8> {
        let list = proto::List {
            items: self.iter().map(|item| item.encode_krpc()).collect(),
        };
        list.encode_to_vec()
    }
}

impl<T: Decode> Decode for Vec<T> {
    fn decode_krpc(client: &ClientRef, bytes: &[u8]) -> Result<Self> {
        let list = proto::List::decode(bytes)?;
        list.items
            .iter()
            .map(|item| T::decode_krpc(client, item))
            .collect()
    }
}

impl<K: Encode, V: Encode> Encode for std::collections::HashMap<K, V> {
    fn encode_krpc(&self) -> Vec<u8> {
        let dict = proto::Dictionary {
            entries: self
                .iter()
                .map(|(k, v)| proto::DictionaryEntry {
                    key: k.encode_krpc(),
                    value: v.encode_krpc(),
                })
                .collect(),
        };
        dict.encode_to_vec()
    }
}

impl<K: Decode + Eq + std::hash::Hash, V: Decode> Decode for std::collections::HashMap<K, V> {
    fn decode_krpc(client: &ClientRef, bytes: &[u8]) -> Result<Self> {
        let dict = proto::Dictionary::decode(bytes)?;
        dict.entries
            .iter()
            .map(|entry| {
                let k = K::decode_krpc(client, &entry.key)?;
                let v = V::decode_krpc(client, &entry.value)?;
                Ok((k, v))
            })
            .collect()
    }
}

impl<T: Encode> Encode for std::collections::HashSet<T> {
    fn encode_krpc(&self) -> Vec<u8> {
        let set = proto::Set {
            items: self.iter().map(|item| item.encode_krpc()).collect(),
        };
        set.encode_to_vec()
    }
}

impl<T: Decode + Eq + std::hash::Hash> Decode for std::collections::HashSet<T> {
    fn decode_krpc(client: &ClientRef, bytes: &[u8]) -> Result<Self> {
        let set = proto::Set::decode(bytes)?;
        set.items
            .iter()
            .map(|item| T::decode_krpc(client, item))
            .collect()
    }
}

// --- Tuple impls ---

fn tuple_item(tuple: &proto::Tuple, index: usize) -> Result<&[u8]> {
    tuple
        .items
        .get(index)
        .map(|item| item.as_slice())
        .ok_or_else(|| malformed(format!("tuple too short: missing item {index}")))
}

macro_rules! impl_codec_for_tuple {
    ($($ty:ident . $idx:tt),+) => {
        impl<$($ty: Encode),+> Encode for ($($ty,)+) {
            fn encode_krpc(&self) -> Vec<u8> {
                let tuple = proto::Tuple {
                    items: vec![$(self.$idx.encode_krpc()),+],
                };
                tuple.encode_to_vec()
            }
        }

        impl<$($ty: Decode),+> Decode for ($($ty,)+) {
            fn decode_krpc(client: &ClientRef, bytes: &[u8]) -> Result<Self> {
                let tuple = proto::Tuple::decode(bytes)?;
                Ok(($($ty::decode_krpc(client, tuple_item(&tuple, $idx)?)?,)+))
            }
        }
    };
}

impl_codec_for_tuple!(A.0, B.1);
impl_codec_for_tuple!(A.0, B.1, C.2);
impl_codec_for_tuple!(A.0, B.1, C.2, D.3);

// --- Protocol message types ---
//
// Some KRPC-service procedures traffic in protocol-level messages
// (e.g. `KRPC.GetStatus` returns a `Status`). These are encoded as
// plain protobuf messages.

macro_rules! impl_codec_for_message {
    ($($ty:ty),+ $(,)?) => {$(
        impl Encode for $ty {
            fn encode_krpc(&self) -> Vec<u8> {
                self.encode_to_vec()
            }
        }

        impl Decode for $ty {
            fn decode_krpc(_client: &ClientRef, bytes: &[u8]) -> Result<Self> {
                Ok(<$ty>::decode(bytes)?)
            }
        }
    )+};
}

// `proto::Event` is absent: the schema's Event type decodes to the typed
// [`crate::Event`] wrapper instead.
impl_codec_for_message!(
    proto::ProcedureCall,
    proto::Stream,
    proto::Status,
    proto::Services,
);

// --- Unit type (for void returns) ---

impl Decode for () {
    fn decode_krpc(_client: &ClientRef, _bytes: &[u8]) -> Result<Self> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::test_client;

    fn round_trip<T: Encode + Decode + PartialEq + std::fmt::Debug>(val: T) {
        let client = test_client();
        assert_eq!(T::decode_krpc(&client, &val.encode_krpc()).unwrap(), val);
    }

    #[test]
    fn round_trip_u32() {
        round_trip(300u32);
    }

    #[test]
    fn round_trip_u64() {
        round_trip(123456789u64);
    }

    #[test]
    fn round_trip_i32() {
        for val in [0i32, 1, -1, 2, -2, i32::MAX, i32::MIN] {
            round_trip(val);
        }
    }

    #[test]
    fn zigzag_known_values() {
        assert_eq!(zigzag_encode32(0), 0);
        assert_eq!(zigzag_encode32(-1), 1);
        assert_eq!(zigzag_encode32(1), 2);
        assert_eq!(zigzag_encode32(-2), 3);
    }

    #[test]
    fn round_trip_i64() {
        for val in [0i64, 1, -1, i64::MAX, i64::MIN] {
            round_trip(val);
        }
    }

    #[test]
    fn round_trip_f32() {
        round_trip(std::f32::consts::PI);
    }

    #[test]
    fn round_trip_f64() {
        round_trip(std::f64::consts::PI);
    }

    #[test]
    fn round_trip_bool() {
        round_trip(true);
        round_trip(false);
    }

    #[test]
    fn round_trip_string() {
        round_trip("hello".to_string());
    }

    #[test]
    fn string_encoding() {
        let encoded = "hello".encode_krpc();
        assert_eq!(encoded, vec![5, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn round_trip_tuple3() {
        round_trip((1.0f64, 2.0f64, 3.0f64));
    }

    #[test]
    fn truncated_string_errors() {
        let client = test_client();
        // Length prefix says 100 bytes but only 5 follow.
        let mut bytes = encode_varint(100);
        bytes.extend_from_slice(b"hello");
        assert!(String::decode_krpc(&client, &bytes).is_err());
        assert!(Vec::<u8>::decode_krpc(&client, &bytes).is_err());
    }

    #[test]
    fn short_tuple_errors() {
        let client = test_client();
        let encoded = (1.0f64, 2.0f64).encode_krpc();
        assert!(<(f64, f64, f64)>::decode_krpc(&client, &encoded).is_err());
    }

    #[test]
    fn wrong_float_width_errors() {
        let client = test_client();
        assert!(f32::decode_krpc(&client, &[0u8; 3]).is_err());
        assert!(f64::decode_krpc(&client, &[0u8; 4]).is_err());
    }
}
