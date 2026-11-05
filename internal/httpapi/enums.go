package httpapi

import (
	"reflect"

	"github.com/danielgtaylor/huma/v2"

	"go-boilerplate/internal/database/dbgen"
)

// enumSchema использует сгенерированный список SQL enum без повторного описания в HTTP-тегах.
func enumSchema[T ~string](values []T) *huma.Schema {
	options := make([]any, len(values))
	for i, v := range values {
		options[i] = string(v)
	}
	return &huma.Schema{Type: "string", Enum: options}
}

type actorTypeSchema string

func (actorTypeSchema) Schema(huma.Registry) *huma.Schema {
	return enumSchema(dbgen.AllActorTypeValues())
}

type auditActionSchema string

func (auditActionSchema) Schema(huma.Registry) *huma.Schema {
	return enumSchema(dbgen.AllAuditActionValues())
}

type auditResultSchema string

func (auditResultSchema) Schema(huma.Registry) *huma.Schema {
	return enumSchema(dbgen.AllAuditResultValues())
}

type fileFormatSchema string

func (fileFormatSchema) Schema(huma.Registry) *huma.Schema {
	return enumSchema(dbgen.AllFileFormatValues())
}

type jobTypeSchema string

func (jobTypeSchema) Schema(huma.Registry) *huma.Schema { return enumSchema(dbgen.AllJobTypeValues()) }

type jobStatusSchema string

func (jobStatusSchema) Schema(huma.Registry) *huma.Schema {
	return enumSchema(dbgen.AllJobStatusValues())
}

type jobErrorCodeSchema string

func (jobErrorCodeSchema) Schema(huma.Registry) *huma.Schema {
	return enumSchema(dbgen.AllJobErrorCodeValues())
}

type jobFileRoleSchema string

func (jobFileRoleSchema) Schema(huma.Registry) *huma.Schema {
	return enumSchema(dbgen.AllJobFileRoleValues())
}

type jobIssueSeveritySchema string

func (jobIssueSeveritySchema) Schema(huma.Registry) *huma.Schema {
	return enumSchema(dbgen.AllJobIssueSeverityValues())
}

func registerEnums(registry huma.Registry) {
	registry.RegisterTypeAlias(reflect.TypeFor[dbgen.ActorType](), reflect.TypeFor[actorTypeSchema]())
	registry.RegisterTypeAlias(reflect.TypeFor[dbgen.AuditAction](), reflect.TypeFor[auditActionSchema]())
	registry.RegisterTypeAlias(reflect.TypeFor[dbgen.AuditResult](), reflect.TypeFor[auditResultSchema]())
	registry.RegisterTypeAlias(reflect.TypeFor[dbgen.FileFormat](), reflect.TypeFor[fileFormatSchema]())
	registry.RegisterTypeAlias(reflect.TypeFor[dbgen.JobType](), reflect.TypeFor[jobTypeSchema]())
	registry.RegisterTypeAlias(reflect.TypeFor[dbgen.JobStatus](), reflect.TypeFor[jobStatusSchema]())
	registry.RegisterTypeAlias(reflect.TypeFor[dbgen.JobErrorCode](), reflect.TypeFor[jobErrorCodeSchema]())
	registry.RegisterTypeAlias(reflect.TypeFor[dbgen.JobFileRole](), reflect.TypeFor[jobFileRoleSchema]())
	registry.RegisterTypeAlias(reflect.TypeFor[dbgen.JobIssueSeverity](), reflect.TypeFor[jobIssueSeveritySchema]())
}
