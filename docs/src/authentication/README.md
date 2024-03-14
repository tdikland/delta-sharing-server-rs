# Authentication

The Delta Sharing server makes use of [bearer tokens](https://datatracker.ietf.org/doc/html/rfc6750) as described in the [Delta Sharing protocol](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md). The owner of the shared data assets is responsible for registering these assets in the Delta Sharing server. The owner of the shared data assets is also responsible for registering new recepients and distributing so-called [Delta Sharing profile files](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#profile-file-format) to recepients. These profiles contain the details for the recipient to connect to the Delta Sharing server with the necessary tokens for authentication.

The bearer token included with the request will be used to authorize the recepient to query the data they have access to.

## Recepients

By recipient we mean the consumer of a given share. Within the Delta Sharing server, the recipient will be identified using the `RecipientId` type. The `RecipientId` can either contain a identifier in the form of a string or set to the anonymous variant. Using the `RecipientId` the server can determine which assets a recipient has access to. Whenever a share provider allows access to the anonymous recipient, the share in essence becomes a public share.

## Public shares

The easiest way to implement authentication for the Delta Sharing server is to allow all recipients access to all shares. Technically this would mean that `RecipientId::Anonymous` has access to the share and that the authentication mechanism would allow unauthenticated requests.

This crate provides authentication middleware to build a Delta Sharing server, but it is also possible to bring your own authentication middleware.