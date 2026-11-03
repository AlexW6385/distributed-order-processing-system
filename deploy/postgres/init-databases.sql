-- Database per service. All four live in one Postgres server for local
-- development, but no service can query another's tables: they only share
-- data through gRPC calls and Kafka events. Each service creates its own
-- schema at startup.
CREATE DATABASE products;
CREATE DATABASE orders;
CREATE DATABASE payments;
CREATE DATABASE notifications;
