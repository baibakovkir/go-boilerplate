package httpapi

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"go-boilerplate/internal/auth"
	"go-boilerplate/internal/database/dbgen"
	"go-boilerplate/internal/jobs"
	"go-boilerplate/internal/security"
	"go-boilerplate/internal/usertasks"
)

// JobOutput возвращает общее состояние задачи любого типа.
type JobOutput struct{ Body jobs.Snapshot }

// JobListOutput содержит только разрешённые текущему аккаунту задачи.
type JobListOutput struct {
	Body struct {
		Items []jobs.Snapshot `json:"items"`
	}
}

// JobIssue представляет диагностику элемента без предметных колонок общей таблицы.
type JobIssue struct {
	ID       int64                  `json:"id"`
	ItemKey  string                 `json:"item_key"`
	Severity dbgen.JobIssueSeverity `json:"severity"`
	Code     dbgen.JobErrorCode     `json:"code"`
	Error    json.RawMessage        `json:"error"`
}

// ExportInput описывает параметры демонстрационного административного endpoint.
type ExportInput struct{ Body usertasks.ExportOptions }

// JobErrorsInput задаёт идентификатор и пагинацию диагностики.
type JobErrorsInput struct {
	IDInput
	Page
}

// JobErrorsOutput возвращает общую диагностику, содержимое error определяет обработчик.
type JobErrorsOutput struct {
	Body struct {
		Items []JobIssue `json:"items"`
	}
}

func (server *Server) registerJobs() {
	op := server.op("POST", "/api/v1/admin/exports", "admin-create-export", "Export users asynchronously", auth.Admin)
	op.DefaultStatus = 202
	huma.Register(server.API, op, func(ctx context.Context, index *ExportInput) (*JobOutput, error) {
		principal := auth.Current(ctx)
		if err := server.Auth.Limit(ctx, "export:"+principal.ID.String(), 5, time.Minute); err != nil {
			return nil, err
		}
		index.Body.Email = auth.NormalizeEmail(index.Body.Email)
		job, err := usertasks.CreateExport(ctx, server.Jobs, jobs.ForPrincipal(principal), index.Body)
		if err != nil {
			return nil, server.Auth.Failure(ctx, "create export", err)
		}
		return &JobOutput{job}, nil
	})
	server.Router.Post("/api/v1/admin/imports/{format}", server.upload)
	schema := server.API.OpenAPI().Components.Schemas.Schema(reflect.TypeFor[jobs.Snapshot](), true, "")
	server.API.OpenAPI().AddOperation(&huma.Operation{
		Method:      "POST",
		Path:        "/api/v1/admin/imports/{format}",
		OperationID: "admin-import-users",
		Tags:        operationTags("/api/v1/admin/imports/{format}"),
		Summary:     "Upload CSV/XLSX",
		Security: []map[string][]string{
			{
				"bearer": {},
			},
		},

		Parameters: []*huma.Param{
			{
				Name:     "format",
				In:       "path",
				Required: true,
				Schema:   enumSchema(dbgen.AllFileFormatValues()),
			},
		},

		RequestBody: &huma.RequestBody{
			Required: true,
			Content: map[string]*huma.MediaType{
				"application/octet-stream": {
					Schema: &huma.Schema{
						Type:   "string",
						Format: "binary",
					},
				},
			},
		},

		Responses: map[string]*huma.Response{
			"202": {
				Description: "Queued job",
				Content: map[string]*huma.MediaType{
					"application/json": {
						Schema: schema,
					},
				},
			},
		},
	})
	server.registerJobRoutes("/api/v1/admin/jobs", auth.Admin)
	server.registerJobRoutes("/api/v1/jobs", auth.User)
}

func (server *Server) visibleJob(ctx context.Context, id uuid.UUID) (dbgen.Job, error) {
	job, err := server.Auth.DB.Queries.GetJob(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return job, huma.Error404NotFound("job not found")
	}
	if err != nil {
		return job, server.Auth.Failure(ctx, "get job", err)
	}
	if !jobs.CanRead(job, auth.Current(ctx)) {
		return job, huma.Error404NotFound("job not found")
	}
	return job, nil
}

func (server *Server) registerJobRoutes(base string, role auth.Role) {
	huma.Register(server.API,
		server.op("GET",
			base,
			string(role)+"-list-jobs",
			"List visible jobs",
			role),
		func(ctx context.Context,
			index *Page) (*JobListOutput,
			error,
		) {
			var rows []dbgen.Job
			var err error
			if role == auth.Admin {
				rows, err = server.Auth.DB.Queries.ListJobs(
					ctx,
					dbgen.ListJobsParams{Limit: index.Limit, Offset: index.Offset},
				)
			} else {
				principal := auth.Current(ctx)
				rows,
					err = server.Auth.DB.Queries.ListOwnJobs(ctx,
					dbgen.ListOwnJobsParams{
						CreatedByUserID: &principal.ID,
						Limit:           index.Limit,
						Offset:          index.Offset,
					})
			}
			if err != nil {
				return nil, server.Auth.Failure(ctx, "list jobs", err)
			}
			out := &JobListOutput{}
			out.Body.Items = []jobs.Snapshot{}
			for _, job := range rows {
				out.Body.Items = append(out.Body.Items, jobs.Public(job))
			}
			return out, nil
		})
	huma.Register(server.API,
		server.op("GET",
			base+"/{id}",
			string(role)+"-get-job",
			"Get job progress",
			role),
		func(ctx context.Context,
			index *IDInput) (*JobOutput,
			error,
		) {
			job, err := server.visibleJob(ctx, index.ID)
			if err != nil {
				return nil, err
			}
			return &JobOutput{jobs.Public(job)}, nil
		})
	huma.Register(server.API,
		server.op("GET",
			base+"/{id}/errors",
			string(role)+"-job-errors",
			"List job issues",
			role),
		func(ctx context.Context,
			job *JobErrorsInput) (*JobErrorsOutput,
			error,
		) {
			jobValue, err := server.visibleJob(ctx, job.ID)
			if err != nil {
				return nil, err
			}
			if jobValue.ArtifactsExpired {
				return nil, huma.NewError(410, "report expired")
			}
			rows,
				err := server.Auth.DB.Queries.ListJobErrors(ctx,
				dbgen.ListJobErrorsParams{
					JobID:  jobValue.ID,
					Limit:  job.Limit,
					Offset: job.Offset,
				})
			if err != nil {
				return nil, server.Auth.Failure(ctx, "list issues", err)
			}
			out := &JobErrorsOutput{}
			out.Body.Items = []JobIssue{}
			for _, issue := range rows {
				out.Body.Items = append(out.Body.Items,
					JobIssue{
						issue.ID,
						issue.ItemKey,
						issue.Severity,
						issue.Code,
						issue.Error,
					})
			}
			return out, nil
		})
	server.Router.Get(base+"/{id}/download", server.download)
	server.Router.Get(base+"/{id}/report", server.report)
	for _, name := range []string{"download", "report"} {
		server.API.OpenAPI().AddOperation(&huma.Operation{
			Method:      "GET",
			Path:        base + "/{id}/" + name,
			OperationID: string(role) + "-job-" + name,
			Tags:        operationTags(base),
			Summary:     "Download " + name,
			Security: []map[string][]string{
				{
					"bearer": {},
				},
			},
			Parameters: []*huma.Param{
				{
					Name:     "id",
					In:       "path",
					Required: true,
					Schema: &huma.Schema{
						Type:   "string",
						Format: "uuid",
					},
				},
			},
			Responses: map[string]*huma.Response{
				"200": {
					Description: "File",
					Content: map[string]*huma.MediaType{
						"application/octet-stream": {
							Schema: &huma.Schema{
								Type:   "string",
								Format: "binary",
							},
						},
					},
				},
			},
		})
	}
}

func (server *Server) upload(response http.ResponseWriter, request *http.Request) {
	request, ok := server.rawAdmin(response, request)
	if !ok {
		return
	}
	format := dbgen.FileFormat(chi.URLParam(request, "format"))
	if !format.Valid() {
		server.rawError(response, request, huma.Error422UnprocessableEntity("unsupported file format"))
		return
	}
	if len(request.URL.Query()) != 0 {
		server.rawError(response, request, huma.Error422UnprocessableEntity("unknown query parameters"))
		return
	}
	if strings.Split(request.Header.Get("Content-Type"), ";")[0] != "application/octet-stream" {
		server.rawError(response, request, huma.NewError(415, "use application/octet-stream"))
		return
	}
	if request.ContentLength > server.Config.Jobs.MaxUploadBytes {
		server.rawError(response, request, huma.NewError(413, "file exceeds upload limit"))
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 15*time.Minute)
	defer cancel()
	request = request.WithContext(ctx)
	_ = http.NewResponseController(response).SetReadDeadline(time.Now().Add(15 * time.Minute))
	defer http.NewResponseController(response).SetReadDeadline(time.Time{})
	snapshot, err := usertasks.CreateImport(
		ctx,
		server.Jobs,
		jobs.ForPrincipal(auth.Current(ctx)),
		format,
		request.Body,
	)
	if errors.Is(err, usertasks.ErrUploadTooLarge) {
		server.rawError(response, request, huma.NewError(413, err.Error()))
		return
	}
	if errors.Is(err, usertasks.ErrEmptyFile) {
		server.rawError(response, request, huma.Error400BadRequest(err.Error()))
		return
	}
	if err != nil {
		server.rawError(response, request, server.Auth.Failure(ctx, "upload import", err))
		return
	}
	writeJSON(response, 202, snapshot)
}

func (server *Server) rawJobPrincipal(response http.ResponseWriter, request *http.Request) (*http.Request, bool) {
	role := auth.User
	if strings.HasPrefix(request.URL.Path, "/api/v1/admin/") {
		role = auth.Admin
	}
	header := request.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		server.rawError(response, request, huma.Error401Unauthorized("bearer token required"))
		return request, false
	}
	principal, err := server.Auth.Authenticate(request.Context(), strings.TrimPrefix(header, "Bearer "), role)
	if err != nil {
		server.rawError(response, request, err)
		return request, false
	}
	return request.WithContext(auth.WithPrincipal(request.Context(), principal)), true
}

func (server *Server) artifactJob(
	response http.ResponseWriter,
	request *http.Request,
) (*http.Request, dbgen.Job, bool) {
	request, ok := server.rawJobPrincipal(response, request)
	if !ok {
		return request, dbgen.Job{}, false
	}
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		server.rawError(response, request, huma.Error422UnprocessableEntity("invalid job id"))
		return request, dbgen.Job{}, false
	}
	job, err := server.visibleJob(request.Context(), id)
	if err != nil {
		server.rawError(response, request, err)
		return request, job, false
	}
	if job.ArtifactsExpired {
		server.rawError(response, request, huma.NewError(410, "artifact expired"))
		return request, job, false
	}
	return request, job, true
}

func (server *Server) download(response http.ResponseWriter, request *http.Request) {
	request, job, ok := server.artifactJob(response, request)
	if !ok {
		return
	}
	data, err := jobs.Decode(job)
	if err != nil {
		server.rawError(response, request, server.Auth.Failure(request.Context(), "decode job files", err))
		return
	}
	artifact, found := data.FindFile(dbgen.JobFileRoleResult)
	if !jobs.Terminal(job.Status) || !found {
		server.rawError(response, request, huma.Error409Conflict("result is not available"))
		return
	}
	path, err := server.Jobs.Path(artifact.Key)
	if err != nil {
		server.rawError(response, request, server.Auth.Failure(request.Context(), "artifact path", err))
		return
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		server.rawError(response, request, huma.NewError(410, "artifact unavailable"))
		return
	}
	if err != nil {
		server.rawError(response, request, server.Auth.Failure(request.Context(), "open artifact", err))
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		server.rawError(response, request, server.Auth.Failure(request.Context(), "stat artifact", err))
		return
	}
	name := artifact.Name
	if name == "" {
		name = "result." + string(artifact.Format)
	}
	response.Header().
		Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name}))
	response.Header().Set("Content-Type", mime.TypeByExtension("."+string(artifact.Format)))
	principal := auth.Current(request.Context())
	server.Auth.Audit(request.Context(),
		principal.Role,
		principal.ID.String(),
		dbgen.AuditActionJobsresultDownloaded,
		job.ID.String(),
		dbgen.AuditResultSuccess)
	http.ServeContent(response, request, name, info.ModTime(), file)
}

func (server *Server) report(response http.ResponseWriter, request *http.Request) {
	request, job, ok := server.artifactJob(response, request)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 10*time.Minute)
	defer cancel()
	rows,
		err := server.Auth.DB.Pool.Query(ctx,
		`
		SELECT item_key, code, error FROM job_issues WHERE job_id=$1 ORDER BY id
	`,
		job.ID)
	if err != nil {
		server.rawError(response, request, server.Auth.Failure(ctx, "open issue report", err))
		return
	}
	defer rows.Close()
	response.Header().Set("Content-Type", "text/csv; charset=utf-8")
	response.Header().Set("Content-Disposition", `attachment; filename="job-issues.csv"`)
	writer := csv.NewWriter(response)
	err = writer.Write([]string{"item_key", "code", "error"})
	for err == nil && rows.Next() {
		var item, code string
		var details []byte
		if err = rows.Scan(&item, &code, &details); err != nil {
			break
		}
		err = writer.Write([]string{security.SafeCSV(item), code, security.SafeCSV(string(details))})
	}
	writer.Flush()
	if err == nil {
		err = rows.Err()
	}
	if err == nil {
		err = writer.Error()
	}
	if err != nil {
		server.Log.ErrorContext(ctx, "stream report", "job_id", job.ID, "error", err)
	}
}
