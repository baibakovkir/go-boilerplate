package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"go-boilerplate/internal/database/dbgen"
)

// RemoveInputs удаляет закрытые входные файлы завершённой задачи и их ключи из data.
func (service *Service) RemoveInputs(ctx context.Context, id uuid.UUID) error {
	job, err := service.DB.Queries.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if !Terminal(job.Status) {
		return nil
	}
	data, err := Decode(job)
	if err != nil {
		return err
	}
	kept := make([]File, 0, len(data.Files))
	for _, file := range data.Files {
		if file.Role != dbgen.JobFileRoleInput {
			kept = append(kept, file)
			continue
		}
		if err = service.removeFile(file); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(kept)
	if err != nil {
		return err
	}
	_, err = service.DB.Pool.Exec(ctx, `
		UPDATE jobs SET data=jsonb_set(data, '{files}', $2, true) WHERE id=$1
	`, id, raw)
	return err
}

func (service *Service) removeFile(file File) error {
	path, err := service.Path(file.Key)
	if err != nil {
		return err
	}
	if err = os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// CleanupFiles очищает файлы и общую диагностику; содержимое Input принадлежит обработчику.
func (service *Service) CleanupFiles(ctx context.Context) error {
	rows,
		err := service.DB.Pool.Query(ctx,
		`
		SELECT id FROM jobs WHERE status IN ('completed', 'completed_with_errors', 'failed')
		AND NOT artifacts_expired
	`)
	if err != nil {
		return err
	}
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()
	for _, id := range ids {
		if err = service.RemoveInputs(ctx, id); err != nil {
			return err
		}
		job, err := service.DB.Queries.GetJob(ctx, id)
		if err != nil {
			return err
		}
		if time.Since(job.UpdatedAt) < service.Config.Jobs.ArtifactTTL {
			continue
		}
		data, err := Decode(job)
		if err != nil {
			return err
		}
		for _, file := range data.Files {
			if err = service.removeFile(file); err != nil {
				return err
			}
		}
		tx, err := service.DB.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `
		DELETE FROM job_issues WHERE job_id=$1
	`, id); err != nil {
			tx.Rollback(ctx)
			return err
		}
		if _,
			err = tx.Exec(ctx,
			`
		UPDATE jobs SET artifacts_expired=true, data=data-'files'-'checkpoint' WHERE id=$1
	`,
			id); err != nil {

			tx.Rollback(ctx)
			return err
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
	}
	entries, err := os.ReadDir(service.Config.Jobs.Directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() && !strings.Contains(entry.Name(), ".work.") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if time.Since(info.ModTime()) < 24*time.Hour {
			continue
		}
		name := entry.Name()
		if len(name) < 36 {
			continue
		}
		id, err := uuid.Parse(name[:36])
		if err != nil {
			continue
		}
		job, err := service.DB.Queries.GetJob(ctx, id)
		keep := false
		if err == nil {
			keep = !Terminal(job.Status)
			if !keep && !job.ArtifactsExpired {
				data, decodeErr := Decode(job)
				if decodeErr != nil {
					return decodeErr
				}
				for _, file := range data.Files {
					if file.Key == name {
						keep = true
					}
				}
			}
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if !keep {
			path := filepath.Join(service.Config.Jobs.Directory, name)
			if entry.IsDir() {
				err = os.RemoveAll(path)
			} else {
				err = os.Remove(path)
			}
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}
