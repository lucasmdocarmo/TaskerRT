//! KEDA external scaler: the control plane tells KEDA exactly how many
//! workers it wants, and KEDA acts on it.

use std::pin::Pin;
use std::sync::Arc;

use tasker_proto::externalscaler::external_scaler_server::ExternalScaler;
use tasker_proto::externalscaler::{
    GetMetricSpecResponse, GetMetricsRequest, GetMetricsResponse, IsActiveResponse, MetricSpec,
    MetricValue, ScaledObjectRef,
};
use tokio::sync::oneshot;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::{Command, Demand, Inbox, Query};

/// The metric name KEDA sees. `targetSize = 1` makes replicas = value.
pub const METRIC: &str = "tasker_desired_workers";

/// Holds the inbox; every call is one `Query::Demand`.
#[derive(Debug)]
pub struct ScalerService {
    inbox: Arc<Inbox>,
}

impl ScalerService {
    #[must_use]
    pub fn new(inbox: Arc<Inbox>) -> Self {
        Self { inbox }
    }

    async fn demand(&self) -> Result<Demand, Status> {
        let (tx, rx) = oneshot::channel();
        self.inbox
            .push(Command::Query(Query::Demand { reply: tx }))
            .map_err(|_| Status::resource_exhausted("ingest ring is full"))?;
        rx.await
            .map_err(|_| Status::unavailable("scheduler stopped"))
    }
}

type ActiveStream = Pin<Box<dyn Stream<Item = Result<IsActiveResponse, Status>> + Send>>;
type SpecStream = Pin<Box<dyn Stream<Item = Result<GetMetricSpecResponse, Status>> + Send>>;

#[tonic::async_trait]
impl ExternalScaler for ScalerService {
    type StreamIsActiveStream = ActiveStream;
    type StreamMetricSpecStream = SpecStream;

    async fn is_active(
        &self,
        _request: Request<ScaledObjectRef>,
    ) -> Result<Response<IsActiveResponse>, Status> {
        let d = self.demand().await?;
        Ok(Response::new(IsActiveResponse {
            result: d.desired > 0,
        }))
    }

    async fn stream_is_active(
        &self,
        _request: Request<ScaledObjectRef>,
    ) -> Result<Response<Self::StreamIsActiveStream>, Status> {
        Err(Status::unimplemented(
            "polling only: use trigger type `external`",
        ))
    }

    async fn get_metric_spec(
        &self,
        _request: Request<ScaledObjectRef>,
    ) -> Result<Response<GetMetricSpecResponse>, Status> {
        Ok(Response::new(GetMetricSpecResponse {
            metric_specs: vec![MetricSpec {
                metric_name: METRIC.into(),
                target_size: 1,
                target_size_float: 0.0,
            }],
        }))
    }

    async fn get_metrics(
        &self,
        _request: Request<GetMetricsRequest>,
    ) -> Result<Response<GetMetricsResponse>, Status> {
        let d = self.demand().await?;
        Ok(Response::new(GetMetricsResponse {
            metric_values: vec![MetricValue {
                metric_name: METRIC.into(),
                metric_value: i64::from(d.desired),
                metric_value_float: 0.0,
            }],
        }))
    }

    async fn stream_metric_spec(
        &self,
        _request: Request<ScaledObjectRef>,
    ) -> Result<Response<Self::StreamMetricSpecStream>, Status> {
        Err(Status::unimplemented(
            "polling only: use trigger type `external`",
        ))
    }
}
