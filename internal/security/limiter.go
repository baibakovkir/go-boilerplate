package security

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var limitScript = redis.NewScript(
	`local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('PEXPIRE',KEYS[1],ARGV[1]) end; return n`,
)

// Allow атомарно учитывает запрос в окне Redis и возвращает результат проверки лимита.
func Allow(ctx context.Context, r *redis.Client, prefix, key string, limit int, window time.Duration) (bool, error) {
	// Хешируем идентификаторы, чтобы email и IP не попадали в открытом виде в имена ключей Redis.
	k := fmt.Sprintf("%slimit:%x", prefix, sha256.Sum256([]byte(key)))
	n, e := limitScript.Run(ctx, r, []string{k}, window.Milliseconds()).Int64()
	return n <= int64(limit), e
}
