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
func (s *Service) RemoveInputs(ctx context.Context, id uuid.UUID) error {
	j, err := s.DB.Q.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if !Terminal(j.Status) {
		return nil
	}
	data, err := Decode(j)
	if err != nil {
		return err
	}
	kept := make([]File, 0, len(data.Files))
	for _, f := range data.Files {
		if f.Role != dbgen.JobFileRoleInput {
			kept = append(kept, f)
			continue
		}
		if err = s.removeFile(f); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(kept)
	if err != nil {
		return err
	}
	_, err = s.DB.Pool.Exec(ctx, `
		UPDATE jobs SET data=jsonb_set(data, '{files}', $2, true) WHERE id=$1
	`, id, raw)
	return err
}

func (s *Service) removeFile(f File) error {
	path, err := s.Path(f.Key)
	if err != nil {
		return err
	}
	if err = os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// CleanupFiles очищает файлы и общую диагностику; содержимое Input принадлежит обработчику.
func (s *Service) CleanupFiles(ctx context.Context) error {
	rows,
		err := s.DB.Pool.Query(ctx,
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
		if err = s.RemoveInputs(ctx, id); err != nil {
			return err
		}
		j, e := s.DB.Q.GetJob(ctx, id)
		if e != nil {
			return e
		}
		if time.Since(j.UpdatedAt) < s.Config.Jobs.ArtifactTTL {
			continue
		}
		data, e := Decode(j)
		if e != nil {
			return e
		}
		for _, f := range data.Files {
			if e = s.removeFile(f); e != nil {
				return e
			}
		}
		tx, e := s.DB.Pool.Begin(ctx)
		if e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, `
		DELETE FROM job_issues WHERE job_id=$1
	`, id); e != nil {
			tx.Rollback(ctx)
			return e
		}
		if _,
			e = tx.Exec(ctx,
			`
		UPDATE jobs SET artifacts_expired=true, data=data-'files'-'checkpoint' WHERE id=$1
	`,
			id); e != nil {

			tx.Rollback(ctx)
			return e
		}
		if e = tx.Commit(ctx); e != nil {
			return e
		}
	}
	entries, err := os.ReadDir(s.Config.Jobs.Directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() && !strings.Contains(entry.Name(), ".work.") {
			continue
		}
		info, e := entry.Info()
		if e != nil {
			return e
		}
		if time.Since(info.ModTime()) < 24*time.Hour {
			continue
		}
		name := entry.Name()
		if len(name) < 36 {
			continue
		}
		id, e := uuid.Parse(name[:36])
		if e != nil {
			continue
		}
		j, e := s.DB.Q.GetJob(ctx, id)
		keep := false
		if e == nil {
			keep = !Terminal(j.Status)
			if !keep && !j.ArtifactsExpired {
				data, decodeErr := Decode(j)
				if decodeErr != nil {
					return decodeErr
				}
				for _, f := range data.Files {
					if f.Key == name {
						keep = true
					}
				}
			}
		} else if !errors.Is(e, pgx.ErrNoRows) {
			return e
		}
		if !keep {
			path := filepath.Join(s.Config.Jobs.Directory, name)
			if entry.IsDir() {
				e = os.RemoveAll(path)
			} else {
				e = os.Remove(path)
			}
			if e != nil && !errors.Is(e, os.ErrNotExist) {
				return e
			}
		}
	}
	return nil
}
