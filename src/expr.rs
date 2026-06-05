//! A builder for server-side expression trees.
//!
//! kRPC [`Expression`]s are constructed node-by-node over RPC, which makes
//! them verbose to assemble by hand. This module provides [`Expr<T>`]: a
//! local, synchronous, *typed* expression tree that issues all of the RPCs
//! in one [`Expr::build`] call at the end.
//!
//! Leaves are literals (via `From`) and procedure calls (via the generated
//! `*_call()` methods, which return a typed [`Call<T>`]). Combine them with
//! comparison methods ([`Expr::gt`], [`Expr::eq`], ...), arithmetic
//! operators (`+ - * / %`), and logical operators (`&`, `|`, `^`, `!`):
//!
//! ```no_run
//! # async fn demo(client: stayputnik::ClientRef,
//! #               sc: stayputnik::services::space_center::SpaceCenter,
//! #               krpc: stayputnik::services::krpc::KRPC) -> stayputnik::Result<()> {
//! use stayputnik::expr::Expr;
//!
//! let expr = Expr::from(sc.ut_call()).gt(100_000.0).build(&client).await?;
//! krpc.add_event(&expr).await?.await?;
//! # Ok(())
//! # }
//! ```
//!
//! The types are advisory but checked: both sides of a comparison must have
//! the same `T`, logical operators require `Expr<bool>`, and
//! [`Expr::cast`] converts between numeric widths explicitly. The server
//! performs the authoritative type check when the tree is built.
//!
//! # Collections and lambdas
//!
//! Expressions over collections (`Expr<Vec<T>>`, from calls returning
//! lists or [`Expr::list`] literals) support iterator-style operations
//! whose closures run *on the server*, evaluated every tick:
//!
//! ```no_run
//! # async fn demo(client: stayputnik::ClientRef,
//! #               laser: stayputnik::services::lidar::Laser,
//! #               krpc: stayputnik::services::krpc::KRPC) -> stayputnik::Result<()> {
//! use stayputnik::expr::Expr;
//!
//! // Fire when any LiDAR return is closer than 50m.
//! let danger_close = Expr::from(laser.cloud_call())
//!     .any(|d| d.lt(50.0))
//!     .build(&client)
//!     .await?;
//! krpc.add_event(&danger_close).await?.await?;
//! # Ok(())
//! # }
//! ```
//!
//! The closure receives an [`Expr`] placeholder for the bound element and
//! returns the body tree; it runs once, locally, during construction.
//! Available: [`map`](Expr::map), [`filter`](Expr::filter),
//! [`any`](Expr::any), [`all`](Expr::all), [`count`](Expr::count),
//! [`contains`](Expr::contains), [`get`](Expr::get), [`sum`](Expr::sum),
//! [`min`](Expr::min), [`max`](Expr::max), [`avg`](Expr::avg),
//! [`reduce`](Expr::reduce), [`fold`](Expr::fold),
//! [`sorted_by`](Expr::sorted_by), and [`concat`](Expr::concat).
//!
//! Two protocol realities to know:
//!
//! - Lambda parameters can only be the five primitive types
//!   ([`ParamType`]); the server cannot bind remote-class elements, so
//!   e.g. iterating a `Vec<Part>` is not representable.
//! - The server type-checks lambdas when the expression is *used* (e.g.
//!   at `add_event`), not when it is built.
//!
//! Building still performs one RPC per node — each node is an object on
//! the server — so prefer building once and reusing the resulting
//! [`Expression`]. Functionality without sugar here remains available on
//! the raw [`Expression`] API and can be embedded in a tree with
//! [`Expr::raw`].

use std::future::Future;
use std::marker::PhantomData;
use std::ops;
use std::pin::Pin;

use crate::services::krpc::{Expression, Type};
use crate::{ClientRef, ProcedureCall, Result};

mod sealed {
    pub trait Sealed {}
}

/// Numeric expression operand types; enables the arithmetic operators.
pub trait Number: sealed::Sealed {
    /// The result type of [`Expr::avg`]. Averaging integers produces a
    /// double, mirroring the server's LINQ semantics.
    type Avg;
}

/// Integer expression operand types; enables the shift operators.
pub trait Integer: Number {}

macro_rules! impl_markers {
    (numbers: $($n:ty => $avg:ty),*; integers: $($i:ty),*) => {
        $(impl sealed::Sealed for $n {} impl Number for $n { type Avg = $avg; })*
        $(impl Integer for $i {})*
    };
}
impl_markers!(
    numbers: f64 => f64, f32 => f32, i32 => f64, i64 => f64, u32 => f64, u64 => f64;
    integers: i32, i64, u32, u64
);

/// A [`ProcedureCall`] tagged with the type of value it returns, as built
/// by the generated `*_call()` methods.
///
/// Convert into a plain [`ProcedureCall`] with `.into()` when the type tag
/// is not needed.
pub struct Call<T> {
    call: ProcedureCall,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Call<T> {
    pub(crate) fn new(call: ProcedureCall) -> Self {
        Self {
            call,
            _marker: PhantomData,
        }
    }
}

impl<T> Clone for Call<T> {
    fn clone(&self) -> Self {
        Self {
            call: self.call.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for Call<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Call").field(&self.call).finish()
    }
}

impl<T> From<Call<T>> for ProcedureCall {
    fn from(call: Call<T>) -> ProcedureCall {
        call.call
    }
}

/// A typed, locally-built expression tree. See the [module docs](self).
pub struct Expr<T> {
    node: Node,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Clone for Expr<T> {
    fn clone(&self) -> Self {
        Self::wrap(self.node.clone())
    }
}

impl<T> std::fmt::Debug for Expr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Expr").field(&self.node).finish()
    }
}

#[derive(Clone, Debug)]
enum Node {
    ConstDouble(f64),
    ConstFloat(f32),
    ConstInt(i32),
    ConstBool(bool),
    ConstString(String),
    ConstList(Vec<Node>),
    Call(ProcedureCall),
    Raw(Expression),
    Unary(UnaryOp, Box<Node>),
    Binary(BinaryOp, Box<Node>, Box<Node>),
    Cast(Box<Node>, TypeKind),
    /// A lambda-bound variable; resolved against the build context.
    /// The declared type lives on the owning [`Lambda`].
    Param(u64),
    /// Collection op taking a lambda (Map, Filter, ...).
    CollLambda(CollLambdaOp, Box<Node>, Lambda),
    /// Fold carries a seed in addition to the lambda.
    Fold(Box<Node>, Box<Node>, Lambda),
    /// Collection op over the whole collection (Count, Sum, ...).
    CollUnary(CollUnaryOp, Box<Node>),
    /// Collection op taking a second scalar/collection operand.
    CollBinary(CollBinaryOp, Box<Node>, Box<Node>),
}

/// A lambda in the local tree: parameter ids/types plus the body that
/// references them via [`Node::Param`].
#[derive(Clone, Debug)]
struct Lambda {
    params: Vec<(u64, TypeKind)>,
    body: Box<Node>,
}

#[derive(Clone, Copy, Debug)]
enum CollLambdaOp {
    Map,
    Filter,
    Any,
    All,
    SortedBy,
    Reduce,
}

#[derive(Clone, Copy, Debug)]
enum CollUnaryOp {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

#[derive(Clone, Copy, Debug)]
enum CollBinaryOp {
    Contains,
    Get,
    Concat,
}

/// Lazy server-side enumerables: the results of these ops lack the
/// concrete-list members that `Count`/`Get` compile against, so the build
/// inserts a `ToList` when one feeds the other.
fn is_lazy(node: &Node) -> bool {
    matches!(
        node,
        Node::CollLambda(CollLambdaOp::Map | CollLambdaOp::Filter | CollLambdaOp::SortedBy, ..)
            | Node::CollBinary(CollBinaryOp::Concat, ..)
    )
}

/// Fresh ids for lambda parameters; uniqueness is all that matters.
fn next_param_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

#[derive(Clone, Copy, Debug)]
enum UnaryOp {
    Not,
}

#[derive(Clone, Copy, Debug)]
enum BinaryOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    And,
    Or,
    Xor,
    Add,
    Sub,
    Mul,
    Div,
    Rem,
    Pow,
    Shl,
    Shr,
}

/// Implementation detail of [`CastTarget`] and [`ParamType`]: the server
/// `Type`s expressible in an expression tree.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub enum TypeKind {
    Double,
    Float,
    Int,
    Bool,
    String,
}

/// Numeric types an expression can be [`Expr::cast`] to.
pub trait CastTarget: sealed::Sealed {
    #[doc(hidden)]
    const KIND: TypeKind;
}

impl CastTarget for f64 {
    const KIND: TypeKind = TypeKind::Double;
}
impl CastTarget for f32 {
    const KIND: TypeKind = TypeKind::Float;
}
impl CastTarget for i32 {
    const KIND: TypeKind = TypeKind::Int;
}

/// Types usable as lambda parameters in collection operations.
///
/// The server can only declare expression parameters of these five
/// primitive types — lambdas over remote-class elements are not
/// representable in the kRPC protocol.
pub trait ParamType: sealed::Sealed {
    #[doc(hidden)]
    const KIND: TypeKind;
}

macro_rules! impl_param_type {
    ($($ty:ty => $kind:ident),* $(,)?) => {$(
        impl ParamType for $ty {
            const KIND: TypeKind = TypeKind::$kind;
        }
    )*};
}
impl_param_type!(f64 => Double, f32 => Float, i32 => Int, bool => Bool, String => String);

impl sealed::Sealed for bool {}
impl sealed::Sealed for String {}

// --- Leaves ---

macro_rules! impl_const_from {
    ($($ty:ty => $node:ident),* $(,)?) => {$(
        impl From<$ty> for Expr<$ty> {
            fn from(value: $ty) -> Self {
                Expr::wrap(Node::$node(value))
            }
        }
    )*};
}
impl_const_from!(f64 => ConstDouble, f32 => ConstFloat, i32 => ConstInt, bool => ConstBool, String => ConstString);

impl From<&str> for Expr<String> {
    fn from(value: &str) -> Self {
        Expr::wrap(Node::ConstString(value.to_string()))
    }
}

impl<T> From<Call<T>> for Expr<T> {
    fn from(call: Call<T>) -> Self {
        Expr::wrap(Node::Call(call.call))
    }
}

// --- Combinators ---

impl<T> Expr<T> {
    fn wrap(node: Node) -> Self {
        Self {
            node,
            _marker: PhantomData,
        }
    }

    fn binary<U>(self, op: BinaryOp, rhs: Expr<T>) -> Expr<U> {
        Expr::wrap(Node::Binary(op, Box::new(self.node), Box::new(rhs.node)))
    }

    /// Embeds an already-built server-side [`Expression`], asserting its
    /// type. The escape hatch for functionality the builder doesn't cover.
    pub fn raw(expression: Expression) -> Self {
        Expr::wrap(Node::Raw(expression))
    }

    /// `self == rhs`
    pub fn eq(self, rhs: impl Into<Expr<T>>) -> Expr<bool> {
        self.binary(BinaryOp::Eq, rhs.into())
    }

    /// `self != rhs`
    pub fn ne(self, rhs: impl Into<Expr<T>>) -> Expr<bool> {
        self.binary(BinaryOp::Ne, rhs.into())
    }

    /// `self > rhs`
    pub fn gt(self, rhs: impl Into<Expr<T>>) -> Expr<bool> {
        self.binary(BinaryOp::Gt, rhs.into())
    }

    /// `self >= rhs`
    pub fn ge(self, rhs: impl Into<Expr<T>>) -> Expr<bool> {
        self.binary(BinaryOp::Ge, rhs.into())
    }

    /// `self < rhs`
    pub fn lt(self, rhs: impl Into<Expr<T>>) -> Expr<bool> {
        self.binary(BinaryOp::Lt, rhs.into())
    }

    /// `self <= rhs`
    pub fn le(self, rhs: impl Into<Expr<T>>) -> Expr<bool> {
        self.binary(BinaryOp::Le, rhs.into())
    }

    /// Converts to another numeric type, via the server's cast machinery.
    pub fn cast<U: CastTarget>(self) -> Expr<U> {
        Expr::wrap(Node::Cast(Box::new(self.node), U::KIND))
    }

    /// Builds the tree on the server, one RPC per node, and returns the
    /// root [`Expression`].
    ///
    /// Note that the server type-checks lambdas when the finished
    /// expression is *used* (e.g. by
    /// [`add_event`](crate::services::krpc::KRPC::add_event)), so some
    /// invalid expressions build successfully and fail there.
    pub async fn build(self, client: &ClientRef) -> Result<Expression> {
        let mut ctx = std::collections::HashMap::new();
        build_node(client, &mut ctx, self.node).await
    }
}

impl<T: Number> Expr<T> {
    /// `self` raised to the power `rhs`.
    pub fn pow(self, rhs: impl Into<Expr<T>>) -> Expr<T> {
        self.binary(BinaryOp::Pow, rhs.into())
    }
}

// --- Collections ---

fn lambda1<P: ParamType, R>(f: impl FnOnce(Expr<P>) -> Expr<R>) -> Lambda {
    let id = next_param_id();
    let body = f(Expr::wrap(Node::Param(id)));
    Lambda {
        params: vec![(id, P::KIND)],
        body: Box::new(body.node),
    }
}

fn lambda2<A: ParamType, B: ParamType, R>(f: impl FnOnce(Expr<A>, Expr<B>) -> Expr<R>) -> Lambda {
    let a = next_param_id();
    let b = next_param_id();
    let body = f(
        Expr::wrap(Node::Param(a)),
        Expr::wrap(Node::Param(b)),
    );
    Lambda {
        params: vec![(a, A::KIND), (b, B::KIND)],
        body: Box::new(body.node),
    }
}

impl<T> Expr<Vec<T>> {
    fn coll_lambda<U>(self, op: CollLambdaOp, lambda: Lambda) -> Expr<U> {
        Expr::wrap(Node::CollLambda(op, Box::new(self.node), lambda))
    }

    fn coll_unary<U>(self, op: CollUnaryOp) -> Expr<U> {
        Expr::wrap(Node::CollUnary(op, Box::new(self.node)))
    }

    /// A literal list of constants, e.g. `Expr::list([1.0, 2.0, 3.0])`.
    pub fn list(items: impl IntoIterator<Item = impl Into<Expr<T>>>) -> Self {
        Expr::wrap(Node::ConstList(
            items.into_iter().map(|item| item.into().node).collect(),
        ))
    }

    /// Transforms each element (kRPC `Select`).
    pub fn map<U>(self, f: impl FnOnce(Expr<T>) -> Expr<U>) -> Expr<Vec<U>>
    where
        T: ParamType,
    {
        let lambda = lambda1(f);
        self.coll_lambda(CollLambdaOp::Map, lambda)
    }

    /// Keeps the elements the predicate holds for (kRPC `Where`).
    pub fn filter(self, f: impl FnOnce(Expr<T>) -> Expr<bool>) -> Expr<Vec<T>>
    where
        T: ParamType,
    {
        let lambda = lambda1(f);
        self.coll_lambda(CollLambdaOp::Filter, lambda)
    }

    /// True if the predicate holds for any element (kRPC `Any`).
    pub fn any(self, f: impl FnOnce(Expr<T>) -> Expr<bool>) -> Expr<bool>
    where
        T: ParamType,
    {
        let lambda = lambda1(f);
        self.coll_lambda(CollLambdaOp::Any, lambda)
    }

    /// True if the predicate holds for every element (kRPC `All`).
    pub fn all(self, f: impl FnOnce(Expr<T>) -> Expr<bool>) -> Expr<bool>
    where
        T: ParamType,
    {
        let lambda = lambda1(f);
        self.coll_lambda(CollLambdaOp::All, lambda)
    }

    /// Sorts ascending by the computed key (kRPC `OrderBy`).
    pub fn sorted_by<K>(self, f: impl FnOnce(Expr<T>) -> Expr<K>) -> Expr<Vec<T>>
    where
        T: ParamType,
    {
        let lambda = lambda1(f);
        self.coll_lambda(CollLambdaOp::SortedBy, lambda)
    }

    /// Folds the elements pairwise, without a seed (kRPC `Aggregate`).
    pub fn reduce(self, f: impl FnOnce(Expr<T>, Expr<T>) -> Expr<T>) -> Expr<T>
    where
        T: ParamType,
    {
        let lambda = lambda2(f);
        self.coll_lambda(CollLambdaOp::Reduce, lambda)
    }

    /// Folds the elements onto a seed (kRPC `AggregateWithSeed`).
    pub fn fold<A: ParamType>(
        self,
        seed: impl Into<Expr<A>>,
        f: impl FnOnce(Expr<A>, Expr<T>) -> Expr<A>,
    ) -> Expr<A>
    where
        T: ParamType,
    {
        let lambda = lambda2(f);
        Expr::wrap(Node::Fold(
            Box::new(self.node),
            Box::new(seed.into().node),
            lambda,
        ))
    }

    /// The number of elements (kRPC `Count`).
    pub fn count(self) -> Expr<i32> {
        self.coll_unary(CollUnaryOp::Count)
    }

    /// True if the collection contains `value` (kRPC `Contains`).
    pub fn contains(self, value: impl Into<Expr<T>>) -> Expr<bool> {
        Expr::wrap(Node::CollBinary(
            CollBinaryOp::Contains,
            Box::new(self.node),
            Box::new(value.into().node),
        ))
    }

    /// The element at `index` (kRPC `Get`).
    pub fn get(self, index: impl Into<Expr<i32>>) -> Expr<T> {
        Expr::wrap(Node::CollBinary(
            CollBinaryOp::Get,
            Box::new(self.node),
            Box::new(index.into().node),
        ))
    }

    /// Both collections, concatenated (kRPC `Concat`).
    pub fn concat(self, other: impl Into<Expr<Vec<T>>>) -> Expr<Vec<T>> {
        Expr::wrap(Node::CollBinary(
            CollBinaryOp::Concat,
            Box::new(self.node),
            Box::new(other.into().node),
        ))
    }
}

impl<T: Number> Expr<Vec<T>> {
    /// The sum of the elements (kRPC `Sum`).
    pub fn sum(self) -> Expr<T> {
        self.coll_unary(CollUnaryOp::Sum)
    }

    /// The smallest element (kRPC `Min`).
    pub fn min(self) -> Expr<T> {
        self.coll_unary(CollUnaryOp::Min)
    }

    /// The largest element (kRPC `Max`).
    pub fn max(self) -> Expr<T> {
        self.coll_unary(CollUnaryOp::Max)
    }

    /// The mean of the elements (kRPC `Average`). Averaging integers
    /// produces a double, mirroring the server's LINQ semantics.
    pub fn avg(self) -> Expr<T::Avg> {
        self.coll_unary(CollUnaryOp::Avg)
    }
}

macro_rules! impl_arith_op {
    ($($trait:ident::$method:ident => $op:ident),* $(,)?) => {$(
        impl<T: Number, R: Into<Expr<T>>> ops::$trait<R> for Expr<T> {
            type Output = Expr<T>;
            fn $method(self, rhs: R) -> Expr<T> {
                self.binary(BinaryOp::$op, rhs.into())
            }
        }
    )*};
}
impl_arith_op!(
    Add::add => Add,
    Sub::sub => Sub,
    Mul::mul => Mul,
    Div::div => Div,
    Rem::rem => Rem,
);

macro_rules! impl_shift_op {
    ($($trait:ident::$method:ident => $op:ident),* $(,)?) => {$(
        impl<T: Integer, R: Into<Expr<T>>> ops::$trait<R> for Expr<T> {
            type Output = Expr<T>;
            fn $method(self, rhs: R) -> Expr<T> {
                self.binary(BinaryOp::$op, rhs.into())
            }
        }
    )*};
}
impl_shift_op!(Shl::shl => Shl, Shr::shr => Shr);

macro_rules! impl_bool_op {
    ($($trait:ident::$method:ident => $op:ident),* $(,)?) => {$(
        impl<R: Into<Expr<bool>>> ops::$trait<R> for Expr<bool> {
            type Output = Expr<bool>;
            fn $method(self, rhs: R) -> Expr<bool> {
                self.binary(BinaryOp::$op, rhs.into())
            }
        }
    )*};
}
impl_bool_op!(BitAnd::bitand => And, BitOr::bitor => Or, BitXor::bitxor => Xor);

impl ops::Not for Expr<bool> {
    type Output = Expr<bool>;
    fn not(self) -> Expr<bool> {
        Expr::wrap(Node::Unary(UnaryOp::Not, Box::new(self.node)))
    }
}

// --- Construction on the server ---

/// Parameter handles created so far during a build. LINQ requires every
/// reference to a lambda parameter to resolve to the *same* server-side
/// object, so each parameter id maps to exactly one `Expression`.
type BuildCtx = std::collections::HashMap<u64, Expression>;

async fn make_type(client: &ClientRef, kind: TypeKind) -> Result<Type> {
    match kind {
        TypeKind::Double => Type::double(client).await,
        TypeKind::Float => Type::float(client).await,
        TypeKind::Int => Type::int(client).await,
        TypeKind::Bool => Type::bool(client).await,
        TypeKind::String => Type::string(client).await,
    }
}

async fn build_lambda(client: &ClientRef, ctx: &mut BuildCtx, lambda: Lambda) -> Result<Expression> {
    let mut params = Vec::new();
    for (id, kind) in &lambda.params {
        let ty = make_type(client, *kind).await?;
        let param = Expression::parameter(client, &format!("p{id}"), &ty).await?;
        ctx.insert(*id, param.clone());
        params.push(param);
    }
    let body = build_node(client, ctx, *lambda.body).await?;
    Expression::function(client, params, &body).await
}

/// Builds a node bottom-up. Boxed for async recursion.
fn build_node<'a>(
    client: &'a ClientRef,
    ctx: &'a mut BuildCtx,
    node: Node,
) -> Pin<Box<dyn Future<Output = Result<Expression>> + Send + 'a>> {
    Box::pin(async move {
        match node {
            Node::ConstDouble(v) => Expression::constant_double(client, v).await,
            Node::ConstFloat(v) => Expression::constant_float(client, v).await,
            Node::ConstInt(v) => Expression::constant_int(client, v).await,
            Node::ConstBool(v) => Expression::constant_bool(client, v).await,
            Node::ConstString(v) => Expression::constant_string(client, &v).await,
            Node::ConstList(items) => {
                let mut built = Vec::new();
                for item in items {
                    built.push(build_node(client, ctx, item).await?);
                }
                Expression::create_list(client, built).await
            }
            Node::Call(call) => Expression::call(client, &call).await,
            Node::Raw(expression) => Ok(expression),
            Node::Param(id) => ctx.get(&id).cloned().ok_or_else(|| {
                crate::Error::Decode(prost::DecodeError::new(
                    "expression lambda parameter used outside its lambda",
                ))
            }),
            Node::Unary(UnaryOp::Not, a) => {
                let a = build_node(client, ctx, *a).await?;
                Expression::not(client, &a).await
            }
            Node::Cast(a, kind) => {
                let a = build_node(client, ctx, *a).await?;
                let ty = make_type(client, kind).await?;
                Expression::cast(client, &a, &ty).await
            }
            Node::CollLambda(op, coll, lambda) => {
                let coll = build_node(client, ctx, *coll).await?;
                let func = build_lambda(client, ctx, lambda).await?;
                match op {
                    CollLambdaOp::Map => Expression::select(client, &coll, &func).await,
                    CollLambdaOp::Filter => Expression::r#where(client, &coll, &func).await,
                    CollLambdaOp::Any => Expression::any(client, &coll, &func).await,
                    CollLambdaOp::All => Expression::all(client, &coll, &func).await,
                    CollLambdaOp::SortedBy => Expression::order_by(client, &coll, &func).await,
                    CollLambdaOp::Reduce => Expression::aggregate(client, &coll, &func).await,
                }
            }
            Node::Fold(coll, seed, lambda) => {
                let coll = build_node(client, ctx, *coll).await?;
                let seed = build_node(client, ctx, *seed).await?;
                let func = build_lambda(client, ctx, lambda).await?;
                Expression::aggregate_with_seed(client, &coll, &seed, &func).await
            }
            Node::CollUnary(op, coll) => {
                // `Count` compiles against concrete-list members that lazy
                // enumerables (map/filter/... results) lack; normalize.
                let needs_list = matches!(op, CollUnaryOp::Count) && is_lazy(&coll);
                let mut coll = build_node(client, ctx, *coll).await?;
                if needs_list {
                    coll = Expression::to_list(client, &coll).await?;
                }
                match op {
                    CollUnaryOp::Count => Expression::count(client, &coll).await,
                    CollUnaryOp::Sum => Expression::sum(client, &coll).await,
                    CollUnaryOp::Min => Expression::min(client, &coll).await,
                    CollUnaryOp::Max => Expression::max(client, &coll).await,
                    CollUnaryOp::Avg => Expression::average(client, &coll).await,
                }
            }
            Node::CollBinary(op, coll, rhs) => {
                // `Get` indexes via concrete-list members; normalize.
                let needs_list = matches!(op, CollBinaryOp::Get) && is_lazy(&coll);
                let mut coll = build_node(client, ctx, *coll).await?;
                if needs_list {
                    coll = Expression::to_list(client, &coll).await?;
                }
                let rhs = build_node(client, ctx, *rhs).await?;
                match op {
                    CollBinaryOp::Contains => Expression::contains(client, &coll, &rhs).await,
                    CollBinaryOp::Get => Expression::get(client, &coll, &rhs).await,
                    CollBinaryOp::Concat => Expression::concat(client, &coll, &rhs).await,
                }
            }
            Node::Binary(op, a, b) => {
                let a = build_node(client, ctx, *a).await?;
                let b = build_node(client, ctx, *b).await?;
                match op {
                    BinaryOp::Eq => Expression::equal(client, &a, &b).await,
                    BinaryOp::Ne => Expression::not_equal(client, &a, &b).await,
                    BinaryOp::Gt => Expression::greater_than(client, &a, &b).await,
                    BinaryOp::Ge => Expression::greater_than_or_equal(client, &a, &b).await,
                    BinaryOp::Lt => Expression::less_than(client, &a, &b).await,
                    BinaryOp::Le => Expression::less_than_or_equal(client, &a, &b).await,
                    BinaryOp::And => Expression::and(client, &a, &b).await,
                    BinaryOp::Or => Expression::or(client, &a, &b).await,
                    BinaryOp::Xor => Expression::exclusive_or(client, &a, &b).await,
                    BinaryOp::Add => Expression::add(client, &a, &b).await,
                    BinaryOp::Sub => Expression::subtract(client, &a, &b).await,
                    BinaryOp::Mul => Expression::multiply(client, &a, &b).await,
                    BinaryOp::Div => Expression::divide(client, &a, &b).await,
                    BinaryOp::Rem => Expression::modulo(client, &a, &b).await,
                    BinaryOp::Pow => Expression::power(client, &a, &b).await,
                    BinaryOp::Shl => Expression::left_shift(client, &a, &b).await,
                    BinaryOp::Shr => Expression::right_shift(client, &a, &b).await,
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec;

    fn call<T>(procedure: &str) -> Call<T> {
        Call::new(codec::call("Test", procedure, vec![]))
    }

    #[test]
    fn comparison_shape() {
        let expr = Expr::from(call::<f64>("Alt")).gt(80_000.0);
        match expr.node {
            Node::Binary(BinaryOp::Gt, lhs, rhs) => {
                assert!(matches!(*lhs, Node::Call(ref c) if c.procedure == "Alt"));
                assert!(matches!(*rhs, Node::ConstDouble(v) if v == 80_000.0));
            }
            other => panic!("unexpected node: {other:?}"),
        }
    }

    #[test]
    fn operator_shape() {
        let throttle = Expr::from(call::<f32>("Throttle"));
        let expr = (throttle + 0.1f32).le(1.0f32) & Expr::from(true);
        match expr.node {
            Node::Binary(BinaryOp::And, lhs, rhs) => {
                assert!(matches!(*rhs, Node::ConstBool(true)));
                match *lhs {
                    Node::Binary(BinaryOp::Le, le_lhs, _) => {
                        assert!(matches!(*le_lhs, Node::Binary(BinaryOp::Add, _, _)));
                    }
                    other => panic!("unexpected node: {other:?}"),
                }
            }
            other => panic!("unexpected node: {other:?}"),
        }
    }

    #[test]
    fn not_and_cast() {
        let expr = !Expr::from(call::<bool>("Flag"));
        assert!(matches!(expr.node, Node::Unary(UnaryOp::Not, _)));

        let cast = Expr::from(call::<i32>("Count")).cast::<f64>();
        assert!(matches!(cast.node, Node::Cast(_, TypeKind::Double)));
    }

    #[test]
    fn lambda_shape() {
        let expr = Expr::from(call::<Vec<f64>>("Cloud")).filter(|d| d.lt(50.0));
        match expr.node {
            Node::CollLambda(CollLambdaOp::Filter, coll, lambda) => {
                assert!(matches!(*coll, Node::Call(_)));
                let (id, kind) = lambda.params[0];
                assert!(matches!(kind, TypeKind::Double));
                // The body references the declared parameter by id.
                match *lambda.body {
                    Node::Binary(BinaryOp::Lt, lhs, _) => {
                        assert!(matches!(*lhs, Node::Param(pid) if pid == id));
                    }
                    other => panic!("unexpected body: {other:?}"),
                }
            }
            other => panic!("unexpected node: {other:?}"),
        }
    }

    #[test]
    fn fresh_param_ids() {
        let a = Expr::from(call::<Vec<f64>>("A")).any(|d| d.gt(0.0));
        let b = Expr::from(call::<Vec<f64>>("B")).any(|d| d.gt(0.0));
        let id_of = |expr: Expr<bool>| match expr.node {
            Node::CollLambda(_, _, lambda) => lambda.params[0].0,
            other => panic!("unexpected node: {other:?}"),
        };
        assert_ne!(id_of(a), id_of(b));
    }

    #[test]
    fn reduce_binds_two_params() {
        let expr = Expr::from(call::<Vec<f64>>("Cloud")).reduce(|acc, d| acc + d);
        match expr.node {
            Node::CollLambda(CollLambdaOp::Reduce, _, lambda) => {
                assert_eq!(lambda.params.len(), 2);
                assert_ne!(lambda.params[0].0, lambda.params[1].0);
            }
            other => panic!("unexpected node: {other:?}"),
        }
    }

    #[test]
    fn lazy_detection() {
        let lazy = Expr::from(call::<Vec<f64>>("Cloud")).filter(|d| d.gt(0.0));
        assert!(is_lazy(&lazy.node));
        let concrete = Expr::<Vec<f64>>::list([1.0, 2.0]);
        assert!(!is_lazy(&concrete.node));
    }
}
