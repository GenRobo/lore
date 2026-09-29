// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use lore_base::types::Context;
use lore_proto::lore::notification::Event;
use lore_proto::lore::notification::PublishRequest;
use lore_revision::lore::RepositoryId;
use lore_telemetry::tracing::fields::REPOSITORY_ID;
use lore_telemetry::tracing::fields::USER_ID;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::debug;
use tracing::instrument;

use crate::auth::Access;
use crate::auth::jwt::remaining_lifetime;
use crate::grpc::TOKEN_EXPIRED;
use crate::grpc::authorize_body_repository;
use crate::grpc::get_authorization;
use crate::grpc::get_user_id;

#[derive(Clone)]
pub struct NotificationService {
    sender: Arc<crate::notification::local::NotificationSender>,
}

impl NotificationService {
    pub fn new(sender: Arc<crate::notification::local::NotificationSender>) -> Self {
        Self { sender }
    }
}

type SubscribeResponseStream =
    Pin<Box<dyn Stream<Item = Result<lore_proto::lore::notification::Event, Status>> + Send>>;

#[async_trait]
impl lore_notification::NotificationService for NotificationService {
    type SubscribeStream = SubscribeResponseStream;

    #[instrument(name = "NotificationService::Subscribe", skip_all)]
    async fn subscribe(
        &self,
        request: Request<lore_proto::lore::notification::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        // AZ-1: the interceptor authorizes the gRPC metadata repository, but a subscription
        // names its repository in the request body, so authorize that one here — otherwise a
        // token for one repository could watch another's events.
        let authorization = get_authorization(request.extensions()).ok();
        let user_id = get_user_id(request.extensions());
        let repository: RepositoryId = Context::from(request.into_inner().repository).into();

        if repository.is_zero() {
            return Err(Status::failed_precondition("invalid stream"));
        }

        authorize_body_repository(
            authorization.as_ref(),
            crate::auth::auth_enabled(),
            repository,
            Access::Read,
        )?;

        let rx = self.sender.register(repository);

        debug!(
            { REPOSITORY_ID } = %repository,
            { USER_ID } = user_id,
            "User subscribed to notifications"
        );

        let events = BroadcastStream::new(rx).filter_map(|res| {
            match res {
                Ok(item) => Some(Ok(item)),
                // Ignore if client is lagging behind, just drop the event
                Err(BroadcastStreamRecvError::Lagged(_)) => None,
            }
        });

        let stream: Self::SubscribeStream = match authorization.as_ref() {
            Some(token) => Box::pin(until_expiry(events, remaining_lifetime(token))),
            None => Box::pin(events),
        };
        Ok(Response::new(stream))
    }

    async fn publish(&self, _request: Request<PublishRequest>) -> Result<Response<()>, Status> {
        Err(Status::permission_denied(
            "Publish is not supported by the local notification service",
        ))
    }
}

/// Yields `events` until the subscription's token expires, then ends the stream with a denial.
fn until_expiry<S>(
    events: S,
    lifetime: Duration,
) -> impl Stream<Item = Result<Event, Status>> + Send + 'static
where
    S: Stream<Item = Result<Event, Status>> + Send + 'static,
{
    async_stream::stream! {
        let expiry = tokio::time::sleep(lifetime);
        tokio::pin!(expiry);
        tokio::pin!(events);
        loop {
            let next = tokio::select! {
                biased;
                () = &mut expiry => None,
                event = events.next() => Some(event),
            };
            match next {
                None => {
                    yield Err(Status::permission_denied(TOKEN_EXPIRED));
                    break;
                }
                Some(Some(event)) => yield event,
                Some(None) => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use lore_base::types::Context;
    use lore_notification::NotificationService as _;
    use lore_proto::lore::notification::SubscribeRequest;
    use lore_revision::lore::RepositoryId;
    use lore_revision::notification::NotificationSender as _;
    use lore_transport::grpc::PARTITION_ID_KEY;
    use tokio_stream::StreamExt;
    use tonic::Code;
    use tonic::Request;
    use tonic::metadata::BinaryMetadataValue;

    use super::NotificationService;
    use crate::auth::jwt::AuthorizationToken;
    use crate::notification::local::NotificationSender;
    use crate::test_support::as_caller;
    use crate::test_support::mint;
    use crate::test_support::urc;

    /// A subscription as the notification interceptor passes it: `header` is the repository
    /// the interceptor authorized, `body` the one the subscription names.
    fn subscription(
        header: RepositoryId,
        body: RepositoryId,
        caller: &(String, AuthorizationToken),
    ) -> Request<SubscribeRequest> {
        let mut request = as_caller(
            SubscribeRequest {
                repository: body.into(),
            },
            Some(caller),
        );
        request.metadata_mut().insert_bin(
            PARTITION_ID_KEY,
            BinaryMetadataValue::from_bytes(header.data()),
        );
        request
    }

    #[tokio::test]
    async fn subscribe_authorizes_the_body_repository_not_the_header() {
        let sender = Arc::new(NotificationSender::default());
        let service = NotificationService::new(sender.clone());
        let own: RepositoryId = rand::random();
        let hidden: RepositoryId = rand::random();
        let caller = mint("alice", &[(&urc(own), &["read"])], 600);

        match service.subscribe(subscription(own, hidden, &caller)).await {
            Ok(_) => panic!("a token scoped to A must not subscribe to R's events"),
            Err(status) => assert_eq!(status.code(), Code::PermissionDenied),
        }

        let mut events = service
            .subscribe(subscription(own, own, &caller))
            .await
            .expect("the caller's own repository")
            .into_inner();
        sender.branch_created(own, rand::random()).await;
        let event = tokio::time::timeout(Duration::from_secs(5), events.next())
            .await
            .expect("an event for A")
            .expect("an open stream")
            .expect("an event, not an error");
        assert_eq!(RepositoryId::from(Context::from(event.repository)), own);
    }

    #[tokio::test]
    async fn a_subscription_ends_when_its_token_expires() {
        let sender = Arc::new(NotificationSender::default());
        let service = NotificationService::new(sender.clone());
        let own: RepositoryId = rand::random();
        let caller = mint("alice", &[(&urc(own), &["read"])], 3);
        let mut events = service
            .subscribe(subscription(own, own, &caller))
            .await
            .expect("subscribe")
            .into_inner();

        sender.branch_created(own, rand::random()).await;
        let before = tokio::time::timeout(Duration::from_secs(2), events.next())
            .await
            .expect("an event while the token is live")
            .expect("an open stream");
        assert!(before.is_ok(), "events flow before expiry: {before:?}");

        let ended = tokio::time::timeout(Duration::from_secs(10), events.next())
            .await
            .expect("the stream must end at the token's expiry, not stay open")
            .expect("expiry ends the stream with a status");
        let status = ended.expect_err("expiry is a denial");
        assert_eq!(status.code(), Code::PermissionDenied);

        sender.branch_created(own, rand::random()).await;
        let after = tokio::time::timeout(Duration::from_secs(1), events.next()).await;
        assert!(
            matches!(after, Ok(None)),
            "no events after expiry: {after:?}"
        );
    }
}
