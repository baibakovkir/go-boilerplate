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

func (s *Server) registerJobs() {
	op := s.op("POST", "/api/v1/admin/exports", "admin-create-export", "Export users asynchronously", auth.Admin)
	op.DefaultStatus = 202
	huma.Register(s.API, op, func(ctx context.Context, i *ExportInput) (*JobOutput, error) {
		p := auth.Current(ctx)
		if err := s.Auth.Limit(ctx, "export:"+p.ID.String(), 5, time.Minute); err != nil {
			return nil, err
		}
		i.Body.Email = auth.NormalizeEmail(i.Body.Email)
		j, err := usertasks.CreateExport(ctx, s.Jobs, jobs.ForPrincipal(p), i.Body)
		if err != nil {
			return nil, s.Auth.Failure(ctx, "create export", err)
		}
		return &JobOutput{j}, nil
	})
	s.Router.Post("/api/v1/admin/imports/{format}", s.upload)
	schema := s.API.OpenAPI().Components.Schemas.Schema(reflect.TypeFor[jobs.Snapshot](), true, "")
	s.API.OpenAPI().AddOperation(&huma.Operation{
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
	s.registerJobRoutes("/api/v1/admin/jobs", auth.Admin)
	s.registerJobRoutes("/api/v1/jobs", auth.User)
}

func (s *Server) visibleJob(ctx context.Context, id uuid.UUID) (dbgen.Job, error) {
	j, err := s.Auth.DB.Q.GetJob(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return j, huma.Error404NotFound("job not found")
	}
	if err != nil {
		return j, s.Auth.Failure(ctx, "get job", err)
	}
	if !jobs.CanRead(j, auth.Current(ctx)) {
		return j, huma.Error404NotFound("job not found")
	}
	return j, nil
}

func (s *Server) registerJobRoutes(base string, role auth.Role) {
	huma.Register(s.API,
		s.op("GET",
			base,
			string(role)+"-list-jobs",
			"List visible jobs",
			role),
		func(ctx context.Context,
			i *Page) (*JobListOutput,
			error,
		) {
			var rows []dbgen.Job
			var err error
			if role == auth.Admin {
				rows, err = s.Auth.DB.Q.ListJobs(ctx, dbgen.ListJobsParams{Limit: i.Limit, Offset: i.Offset})
			} else {
				p := auth.Current(ctx)
				rows,
					err = s.Auth.DB.Q.ListOwnJobs(ctx,
					dbgen.ListOwnJobsParams{
						CreatedByUserID: &p.ID,
						Limit:           i.Limit,
						Offset:          i.Offset,
					})
			}
			if err != nil {
				return nil, s.Auth.Failure(ctx, "list jobs", err)
			}
			out := &JobListOutput{}
			out.Body.Items = []jobs.Snapshot{}
			for _, j := range rows {
				out.Body.Items = append(out.Body.Items, jobs.Public(j))
			}
			return out, nil
		})
	huma.Register(s.API,
		s.op("GET",
			base+"/{id}",
			string(role)+"-get-job",
			"Get job progress",
			role),
		func(ctx context.Context,
			i *IDInput) (*JobOutput,
			error,
		) {
			j, err := s.visibleJob(ctx, i.ID)
			if err != nil {
				return nil, err
			}
			return &JobOutput{jobs.Public(j)}, nil
		})
	huma.Register(s.API,
		s.op("GET",
			base+"/{id}/errors",
			string(role)+"-job-errors",
			"List job issues",
			role),
		func(ctx context.Context,
			i *JobErrorsInput) (*JobErrorsOutput,
			error,
		) {
			j, err := s.visibleJob(ctx, i.ID)
			if err != nil {
				return nil, err
			}
			if j.ArtifactsExpired {
				return nil, huma.NewError(410, "report expired")
			}
			rows,
				err := s.Auth.DB.Q.ListJobErrors(ctx,
				dbgen.ListJobErrorsParams{
					JobID:  j.ID,
					Limit:  i.Limit,
					Offset: i.Offset,
				})
			if err != nil {
				return nil, s.Auth.Failure(ctx, "list issues", err)
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
	s.Router.Get(base+"/{id}/download", s.download)
	s.Router.Get(base+"/{id}/report", s.report)
	for _, name := range []string{"download", "report"} {
		s.API.OpenAPI().AddOperation(&huma.Operation{
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

func (s *Server) upload(w http.ResponseWriter, r *http.Request) {
	r, ok := s.rawAdmin(w, r)
	if !ok {
		return
	}
	format := dbgen.FileFormat(chi.URLParam(r, "format"))
	if !format.Valid() {
		s.rawError(w, r, huma.Error422UnprocessableEntity("unsupported file format"))
		return
	}
	if len(r.URL.Query()) != 0 {
		s.rawError(w, r, huma.Error422UnprocessableEntity("unknown query parameters"))
		return
	}
	if strings.Split(r.Header.Get("Content-Type"), ";")[0] != "application/octet-stream" {
		s.rawError(w, r, huma.NewError(415, "use application/octet-stream"))
		return
	}
	if r.ContentLength > s.Config.Jobs.MaxUploadBytes {
		s.rawError(w, r, huma.NewError(413, "file exceeds upload limit"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Minute)
	defer cancel()
	r = r.WithContext(ctx)
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(15 * time.Minute))
	defer http.NewResponseController(w).SetReadDeadline(time.Time{})
	snapshot, err := usertasks.CreateImport(ctx, s.Jobs, jobs.ForPrincipal(auth.Current(ctx)), format, r.Body)
	if errors.Is(err, usertasks.ErrUploadTooLarge) {
		s.rawError(w, r, huma.NewError(413, err.Error()))
		return
	}
	if errors.Is(err, usertasks.ErrEmptyFile) {
		s.rawError(w, r, huma.Error400BadRequest(err.Error()))
		return
	}
	if err != nil {
		s.rawError(w, r, s.Auth.Failure(ctx, "upload import", err))
		return
	}
	writeJSON(w, 202, snapshot)
}

func (s *Server) rawJobPrincipal(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	role := auth.User
	if strings.HasPrefix(r.URL.Path, "/api/v1/admin/") {
		role = auth.Admin
	}
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		s.rawError(w, r, huma.Error401Unauthorized("bearer token required"))
		return r, false
	}
	p, err := s.Auth.Authenticate(r.Context(), strings.TrimPrefix(header, "Bearer "), role)
	if err != nil {
		s.rawError(w, r, err)
		return r, false
	}
	return r.WithContext(auth.WithPrincipal(r.Context(), p)), true
}

func (s *Server) artifactJob(w http.ResponseWriter, r *http.Request) (*http.Request, dbgen.Job, bool) {
	r, ok := s.rawJobPrincipal(w, r)
	if !ok {
		return r, dbgen.Job{}, false
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.rawError(w, r, huma.Error422UnprocessableEntity("invalid job id"))
		return r, dbgen.Job{}, false
	}
	j, err := s.visibleJob(r.Context(), id)
	if err != nil {
		s.rawError(w, r, err)
		return r, j, false
	}
	if j.ArtifactsExpired {
		s.rawError(w, r, huma.NewError(410, "artifact expired"))
		return r, j, false
	}
	return r, j, true
}

func (s *Server) download(w http.ResponseWriter, r *http.Request) {
	r, j, ok := s.artifactJob(w, r)
	if !ok {
		return
	}
	data, err := jobs.Decode(j)
	if err != nil {
		s.rawError(w, r, s.Auth.Failure(r.Context(), "decode job files", err))
		return
	}
	artifact, found := data.FindFile(dbgen.JobFileRoleResult)
	if !jobs.Terminal(j.Status) || !found {
		s.rawError(w, r, huma.Error409Conflict("result is not available"))
		return
	}
	path, err := s.Jobs.Path(artifact.Key)
	if err != nil {
		s.rawError(w, r, s.Auth.Failure(r.Context(), "artifact path", err))
		return
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		s.rawError(w, r, huma.NewError(410, "artifact unavailable"))
		return
	}
	if err != nil {
		s.rawError(w, r, s.Auth.Failure(r.Context(), "open artifact", err))
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		s.rawError(w, r, s.Auth.Failure(r.Context(), "stat artifact", err))
		return
	}
	name := artifact.Name
	if name == "" {
		name = "result." + string(artifact.Format)
	}
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name}))
	w.Header().Set("Content-Type", mime.TypeByExtension("."+string(artifact.Format)))
	p := auth.Current(r.Context())
	s.Auth.Audit(r.Context(),
		p.Role,
		p.ID.String(),
		dbgen.AuditActionJobsresultDownloaded,
		j.ID.String(),
		dbgen.AuditResultSuccess)
	http.ServeContent(w, r, name, info.ModTime(), f)
}

func (s *Server) report(w http.ResponseWriter, r *http.Request) {
	r, j, ok := s.artifactJob(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	rows,
		err := s.Auth.DB.Pool.Query(ctx,
		`
		SELECT item_key, code, error FROM job_issues WHERE job_id=$1 ORDER BY id
	`,
		j.ID)
	if err != nil {
		s.rawError(w, r, s.Auth.Failure(ctx, "open issue report", err))
		return
	}
	defer rows.Close()
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="job-issues.csv"`)
	writer := csv.NewWriter(w)
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
		s.Log.ErrorContext(ctx, "stream report", "job_id", j.ID, "error", err)
	}
}
