-- +goose Up
CREATE TABLE users (
 id uuid PRIMARY KEY,
 email text NOT NULL UNIQUE CHECK (email = lower(btrim(email))),
 password_hash text NOT NULL,
 blocked boolean NOT NULL DEFAULT false,
 auth_version bigint NOT NULL DEFAULT 1,
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE admin_users (LIKE users INCLUDING ALL);
-- Замкнутые наборы видны непосредственно в схеме PostgreSQL.
CREATE TYPE actor_type AS ENUM ('user', 'admin', 'system');
CREATE TYPE audit_result AS ENUM ('success', 'denied', 'failure', 'partial');
CREATE TYPE audit_action AS ENUM (
 'auth.login', 'auth.logout', 'account.password_changed', 'account.password_reset',
 'users.blocked', 'users.unblocked', 'users.sessions_revoked',
 'admin.tables.viewed', 'admin.kafka.viewed',
 'jobs.created', 'jobs.finished', 'jobs.failed', 'jobs.result_downloaded'
);
CREATE TYPE job_type AS ENUM ('users.import', 'users.export');
CREATE TYPE job_status AS ENUM ('queued', 'running', 'completed', 'completed_with_errors', 'failed');
CREATE TYPE file_format AS ENUM ('csv', 'xlsx');
CREATE TYPE job_file_role AS ENUM ('input', 'result', 'report');
CREATE TYPE job_issue_severity AS ENUM ('warning', 'error');
CREATE TYPE job_error_code AS ENUM (
 'none', 'invalid_file', 'invalid_header', 'row_limit_exceeded', 'empty_file',
 'invalid_checkpoint', 'invalid_columns', 'invalid_credentials', 'email_exists',
 'retry_pending', 'processing_failed', 'attempts_exhausted', 'unknown_type', 'invalid_payload'
);
CREATE TABLE audit_logs (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 actor_type actor_type NOT NULL,
 actor_id text NOT NULL DEFAULT '',
 action audit_action NOT NULL,
 object_id text NOT NULL DEFAULT '',
 result audit_result NOT NULL,
 request_id text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX audit_logs_created_at_idx ON audit_logs(created_at);
CREATE TABLE jobs (
 id uuid PRIMARY KEY,
 type job_type NOT NULL,
 status job_status NOT NULL DEFAULT 'queued',
 created_by_user_id uuid REFERENCES users(id),
 created_by_admin_id uuid REFERENCES admin_users(id),
 CHECK (num_nonnulls(created_by_user_id, created_by_admin_id) <= 1),
 -- Оба FK пусты у системной задачи; параметры принадлежат конкретному обработчику.
 data jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(data) = 'object'),
 error_code job_error_code NOT NULL DEFAULT 'none',
 error jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(error) = 'object'),
 processed bigint NOT NULL DEFAULT 0 CHECK (processed >= 0),
 succeeded bigint NOT NULL DEFAULT 0 CHECK (succeeded >= 0),
 failed bigint NOT NULL DEFAULT 0 CHECK (failed >= 0),
 total bigint NOT NULL DEFAULT 0 CHECK (total >= 0),
 version bigint NOT NULL DEFAULT 1,
 attempts integer NOT NULL DEFAULT 0,
 next_attempt_at timestamptz NOT NULL DEFAULT now(),
 lease_owner text NOT NULL DEFAULT '',
 lease_until timestamptz NOT NULL DEFAULT now(),
 artifacts_expired boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX jobs_recovery_idx ON jobs(status,next_attempt_at,lease_until);
CREATE INDEX jobs_user_idx ON jobs(created_by_user_id,created_at DESC);
CREATE INDEX jobs_admin_idx ON jobs(created_by_admin_id,created_at DESC);
CREATE TABLE job_issues (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
 item_key text NOT NULL,
 severity job_issue_severity NOT NULL DEFAULT 'error',
 code job_error_code NOT NULL CHECK (code <> 'none'),
 error jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(error) = 'object'),
 UNIQUE(job_id,item_key,code)
);
CREATE INDEX job_issues_job_idx ON job_issues(job_id,id);
CREATE TABLE outbox (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
 available_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX outbox_available_idx ON outbox(available_at);

-- +goose Down
DROP TABLE outbox,job_issues,jobs,audit_logs,admin_users,users;
DROP TYPE job_error_code,job_issue_severity,job_file_role,file_format,job_status,job_type;
DROP TYPE audit_action,audit_result,actor_type;
