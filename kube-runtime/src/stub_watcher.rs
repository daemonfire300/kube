use futures::StreamExt;
use kube_client::{
    Resource,
    api::{ListParams, ObjectList, TypeMeta, WatchEvent, WatchParams},
};
use serde::de::DeserializeOwned;
use std::{
    cell::{Ref, RefCell},
    collections::VecDeque,
    fmt::Debug,
    pin::Pin,
    task::ready,
};

use crate::watcher::ApiMode;

pub enum Recording {
    List(ListParams),
    Watch(WatchParams, String),
}

/// `TestMode` is the test-only "mock" implementation for [`ApiMode`].
///
pub struct TestMode<K>
where
    K: Clone + Debug + DeserializeOwned + Send + 'static,
{
    /// [`TestMode::list_sequence`] is the fixed list of values `TestMode` returns, removing
    /// one element per call and returning it once none are left.
    /// This enables us to simulate different list scenarios.
    ///
    list_sequence: RefCell<VecDeque<kube_client::Result<ObjectList<K>>>>,
    /// [`TestMode::watch_sequences`] is the fixed list of [`Sequence`]s `TestMode` returns. The
    /// behaviour, is similar to [`TestMode::list_sequence`] but it allows for simulating "waiting" periods,
    /// empty intermediary result and for returning a [`futures::stream::BoxStream`] implemented via [`TestStream`].
    watch_sequences: RefCell<VecDeque<Sequence<K>>>,
    recorder: RefCell<Vec<Recording>>,
}

impl<K> TestMode<K>
where
    K: Clone + Debug + DeserializeOwned + Send,
{
    pub fn get_recordings(&self) -> Ref<'_, Vec<Recording>> {
        self.recorder.borrow()
    }
}

impl<K> TestMode<K>
where
    K: Clone + Debug + DeserializeOwned + Send,
{
    /// Arguments are mut because we reverse the order internally because we pop off the end
    /// which means the first element to be returned would the last which would be unexpected.
    pub fn new(
        fixture: VecDeque<kube_client::Result<ObjectList<K>>>,
        watch_sequence: VecDeque<Sequence<K>>,
    ) -> Self {
        Self {
            list_sequence: RefCell::new(fixture),
            watch_sequences: RefCell::new(watch_sequence),
            recorder: RefCell::new(vec![]),
        }
    }
}

pub struct ResultPage<K>
where
    K: Clone,
{
    inner: ObjectList<K>,
}

impl<K> ResultPage<K>
where
    K: Clone + Resource<DynamicType = ()>,
{
    pub fn empty() -> Self {
        ResultPage { inner: empty_list() }
    }

    pub fn continue_token(mut self, token: Option<&str>) -> Self {
        self.inner.metadata.continue_ = token.map(str::to_string);
        self
    }

    pub fn resource_version(mut self, version: Option<&str>) -> Self {
        self.inner.metadata.resource_version = version.map(str::to_string);
        self
    }

    pub fn items(mut self, items: Vec<K>) -> Self {
        self.inner.items = items;
        self
    }
}

impl<K> From<ResultPage<K>> for ObjectList<K>
where
    K: Clone + Resource<DynamicType = ()>,
{
    fn from(value: ResultPage<K>) -> Self {
        value.inner
    }
}

fn empty_list<K>() -> ObjectList<K>
where
    K: Clone + Resource<DynamicType = ()>,
{
    ObjectList {
        types: TypeMeta::list::<K>(),
        metadata: kube_client::api::ListMeta::default(),
        items: Vec::new(),
    }
}

/// Utility enum to represent different "Segments" of a continuum over repeated calls on a running watch(er).
pub enum SequenceStep<K> {
    /// Represents returning from a list of results until the inner list is empty, i.e., [`std::task::Poll::Ready`] with one [`kube_client::Result<WatchEvent<_>>`]
    /// for each call.
    List(VecDeque<kube_client::Result<WatchEvent<K>>>),
    /// Represents a "sleep"/wait behaviour to simulate a watch(er) not returning elements for a
    /// certain duration.
    Wait(std::time::Duration),
}

pub struct Sequence<K> {
    inner: VecDeque<SequenceStep<K>>,
}

impl<K> Sequence<K> {
    pub fn new(steps: VecDeque<SequenceStep<K>>) -> Self {
        Self { inner: steps }
    }
}

impl<K> Default for Sequence<K> {
    fn default() -> Self {
        Sequence {
            inner: VecDeque::new(),
        }
    }
}

/// Implements [`futures::stream::BoxStream`] via [`futures::Stream`] for internal use via [`TestMode::watch`]
pub struct TestStream<K> {
    seq: RefCell<Sequence<K>>,
    /// [`TestStream::waiting`] is an internal field to support a [`Sequence<K>`] with
    /// any item being [`SequenceStep::Wait`]. Where the inner-most [`tokio::time::Sleep`]
    waiting: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<K: Unpin> futures::Stream for TestStream<K> {
    type Item = kube_client::Result<WatchEvent<K>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // The loop is here for handling the Wait/sleep case ergonomically:
        // Once we encounter a sleep we want to go back to polling it:
        // If it is pending (`std::task::Pending`), the macro neatly yields back to the executor for us and the call to
        // poll(cx) assumes the the sleep has registered its waker into our cx (context), therefore
        // waking (polling) the stream at the appropriate moment again.
        // If it is ready (`std::task:Read(())`) immeadiately we want to continue consuming the next
        // item in our sequence
        loop {
            if let Some(sleep) = this.waiting.as_mut() {
                ready!(sleep.as_mut().poll(cx));
                this.waiting = None;
            }
            let mut seq = this.seq.borrow_mut();
            match seq.inner.pop_front() {
                Some(step) => match step {
                    SequenceStep::List(mut watch_events) => {
                        return std::task::Poll::Ready(watch_events.pop_front());
                    }
                    SequenceStep::Wait(duration) => {
                        if this.waiting.is_some() {
                            unreachable!(
                                "TestStream::waiting should be None when accessing inner, this is a bug"
                            )
                        }
                        this.waiting = Some(Box::pin(tokio::time::sleep(duration)));
                    }
                },
                None => return std::task::Poll::Ready(None), // terminate the stream because no
                                                             // squence steps are left
            }
        }
    }
}

#[allow(clippy::unused_async_trait_impl)]
impl<K> ApiMode for TestMode<K>
where
    K: Resource<DynamicType = ()> + Clone + Debug + DeserializeOwned + Send + Unpin + 'static,
{
    type Value = K;

    async fn list(&self, lp: &ListParams) -> kube_client::Result<ObjectList<Self::Value>> {
        self.recorder.borrow_mut().push(Recording::List(lp.clone()));
        match self.list_sequence.borrow_mut().pop_front() {
            Some(next) => next,
            None => Ok(empty_list()),
        }
    }

    async fn watch(
        &self,
        wp: &WatchParams,
        version: &str,
    ) -> kube_client::Result<futures::stream::BoxStream<'static, kube_client::Result<WatchEvent<Self::Value>>>>
    {
        self.recorder
            .borrow_mut()
            .push(Recording::Watch(wp.clone(), version.into()));
        let seq = self.watch_sequences.borrow_mut().pop_front().unwrap_or_default();
        // NOTE(juf): Currently we assume no sequences == empty
        // stream.
        Ok(TestStream {
            seq: RefCell::new(seq),
            waiting: None,
        }
        .fuse()
        .boxed())
    }
}
