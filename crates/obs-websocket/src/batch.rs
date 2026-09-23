//! Request batches (op 8) on the same session as typed requests.

use obs_websocket_core::{BatchItemResult, RawCall, Request, RequestBatchExecutionType};
use serde_json::Value;

use crate::client::Client;
use crate::error::Error;

/// A batch built with [`Client::batch`].
pub struct Batch<'a> {
    client: &'a Client,
    calls: Vec<RawCall>,
    halt_on_failure: bool,
    execution_type: i64,
}

impl<'a> Batch<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self {
            client,
            calls: Vec::new(),
            halt_on_failure: false,
            execution_type: RequestBatchExecutionType::SerialRealtime.as_i64(),
        }
    }

    /// Appends a typed request.
    ///
    /// Named `add` so batches read as `batch().add(..).add_raw(..)`.
    #[allow(clippy::should_implement_trait)]
    pub fn add<R: Request>(mut self, request: &R) -> Self {
        let data = serde_json::to_value(request).unwrap_or(Value::Null);
        self.calls.push(RawCall::new(R::REQUEST_TYPE, data));
        self
    }

    /// Appends an untyped request.
    pub fn add_raw(mut self, call: RawCall) -> Self {
        self.calls.push(call);
        self
    }

    /// Sets `executionType`. The default is [`RequestBatchExecutionType::SerialRealtime`].
    pub fn execution(mut self, execution: RequestBatchExecutionType) -> Self {
        self.execution_type = execution.as_i64();
        self
    }

    /// Sets `haltOnFailure`.
    pub fn halt_on_failure(mut self, halt: bool) -> Self {
        self.halt_on_failure = halt;
        self
    }

    /// Sends op 8 and waits for op 9.
    pub async fn send(self) -> Result<Vec<BatchItemResult>, Error> {
        for call in &self.calls {
            self.client.ensure_supported(&call.request_type)?;
        }
        self.client
            .dispatch_batch(self.calls, self.halt_on_failure, self.execution_type)
            .await
    }
}
