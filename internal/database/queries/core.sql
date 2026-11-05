-- name: GetUser :one
SELECT * FROM users WHERE id=$1;
-- name: GetUserByEmail :one
SELECT * FROM users WHERE email=$1;
-- name: GetAdmin :one
SELECT * FROM admin_users WHERE id=$1;
-- name: GetAdminByEmail :one
SELECT * FROM admin_users WHERE email=$1;
-- name: CreateAdmin :exec
INSERT INTO admin_users(id,email,password_hash) VALUES($1,$2,$3) ON CONFLICT(email) DO NOTHING;
-- name: ChangeUserPassword :execrows
UPDATE users SET password_hash=$2,auth_version=auth_version+1,updated_at=now() WHERE id=$1 AND auth_version=$3;
-- name: ChangeAdminPassword :execrows
UPDATE admin_users SET password_hash=$2,auth_version=auth_version+1,updated_at=now() WHERE id=$1 AND auth_version=$3;
-- name: SetUserBlocked :execrows
UPDATE users SET blocked=$2,auth_version=auth_version+1,updated_at=now() WHERE id=$1;
-- name: RevokeUserSessions :execrows
UPDATE users SET auth_version=auth_version+1,updated_at=now() WHERE id=$1;
-- name: InsertAudit :exec
INSERT INTO audit_logs(actor_type,actor_id,action,object_id,result,request_id) VALUES($1,$2,$3,$4,$5,$6);
-- name: ListUsers :many
SELECT id,email,blocked,created_at,updated_at FROM users
WHERE (sqlc.arg(email_filter)::text='' OR strpos(email,sqlc.arg(email_filter)::text)>0)
 AND (sqlc.arg(blocked_filter)::text='' OR blocked::text=sqlc.arg(blocked_filter)::text)
ORDER BY created_at DESC,id DESC LIMIT $1 OFFSET $2;
-- name: ListJobs :many
SELECT * FROM jobs ORDER BY created_at DESC,id DESC LIMIT $1 OFFSET $2;
-- name: GetJob :one
SELECT * FROM jobs WHERE id=$1;
-- name: ListJobErrors :many
SELECT * FROM job_issues WHERE job_id=$1 ORDER BY id LIMIT $2 OFFSET $3;
-- name: ListAudit :many
SELECT * FROM audit_logs WHERE (sqlc.arg(action_filter)::text='' OR action::text=sqlc.arg(action_filter)::text) ORDER BY id DESC LIMIT $1 OFFSET $2;

-- name: ListOwnJobs :many
SELECT * FROM jobs WHERE created_by_user_id=$1 ORDER BY created_at DESC,id DESC LIMIT $2 OFFSET $3;
