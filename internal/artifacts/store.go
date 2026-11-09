// Package artifacts хранит закрытые артефакты задач в S3 без привязки к локальному диску.
package artifacts

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	appconfig "go-boilerplate/internal/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// Object содержит метаданные объекта без credentials и адреса хранилища.
type Object struct {
	Key      string
	Size     int64
	Modified time.Time
	ETag     string
}

// Reader предоставляет поток и произвольное позиционирование для HTTP Range.
type Reader interface {
	io.ReadSeekCloser
	Info() Object
}

// Store определяет операции с приватными объектами. Удаление отсутствующего ключа успешно.
type Store interface {
	Put(context.Context, string, io.ReadSeeker, int64) error
	Open(context.Context, string) (Reader, error)
	Delete(context.Context, string) error
	List(context.Context, string) ([]Object, string, error)
	Check(context.Context) error
}

// S3 реализует Store через AWS SDK. Подключения HTTP освобождаются вместе с телами ответов.
type S3 struct {
	client         *s3.Client
	bucket, prefix string
}

// New создаёт S3-клиент; credentials без явной пары берутся из стандартной цепочки AWS.
func New(ctx context.Context, config appconfig.S3Config) (*S3, error) {
	options := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(config.Region)}
	if config.AccessKey != "" {
		options = append(
			options,
			awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(config.AccessKey, config.SecretKey, ""),
			),
		)
	}
	loaded, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(loaded, func(options *s3.Options) {
		options.UsePathStyle = config.PathStyle
		if config.Endpoint != "" {
			options.BaseEndpoint = aws.String(config.Endpoint)
		}
		options.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		options.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return &S3{client: client, bucket: config.Bucket, prefix: strings.Trim(config.Prefix, "/")}, nil
}

func (store *S3) key(key string) (string, error) {
	if key == "" || path.Base(key) != key || strings.ContainsAny(key, "/\\") || key == "." || key == ".." {
		return "", fmt.Errorf("invalid artifact key")
	}
	if store.prefix != "" {
		return store.prefix + "/" + key, nil
	}
	return key, nil
}

// Put загружает файл одним атомарно видимым PutObject без multipart-состояния.
// Поток должен поддерживать Seek для подписи и повторов SDK; содержимое не буферизуется целиком.
func (store *S3) Put(ctx context.Context, key string, body io.ReadSeeker, size int64) error {
	objectKey, err := store.key(key)
	if err != nil {
		return err
	}
	_, err = store.client.PutObject(
		ctx,
		&s3.PutObjectInput{
			Bucket:        aws.String(store.bucket),
			Key:           aws.String(objectKey),
			Body:          body,
			ContentLength: aws.Int64(size),
		},
	)
	return err
}

// Open получает метаданные, открывая поток лениво при первом Read.
func (store *S3) Open(ctx context.Context, key string) (Reader, error) {
	objectKey, err := store.key(key)
	if err != nil {
		return nil, err
	}
	head, err := store.client.HeadObject(
		ctx,
		&s3.HeadObjectInput{Bucket: aws.String(store.bucket), Key: aws.String(objectKey)},
	)
	if err != nil {
		return nil, notFound(err)
	}
	return &reader{
		store: store,
		ctx:   ctx,
		key:   objectKey,
		info: Object{
			Key:      key,
			Size:     aws.ToInt64(head.ContentLength),
			Modified: aws.ToTime(head.LastModified),
			ETag:     aws.ToString(head.ETag),
		},
	}, nil
}

// Delete безопасно повторяется после частично выполненной очистки.
func (store *S3) Delete(ctx context.Context, key string) error {
	objectKey, err := store.key(key)
	if err != nil {
		return err
	}
	_, err = store.client.DeleteObject(
		ctx,
		&s3.DeleteObjectInput{Bucket: aws.String(store.bucket), Key: aws.String(objectKey)},
	)
	if errors.Is(notFound(err), os.ErrNotExist) {
		return nil
	}
	return err
}

// List возвращает одну страницу объектов в выделенном префиксе и следующий курсор.
func (store *S3) List(ctx context.Context, cursor string) ([]Object, string, error) {
	prefix := store.prefix
	if prefix != "" {
		prefix += "/"
	}
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(store.bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(1000),
	}
	if cursor != "" {
		input.ContinuationToken = aws.String(cursor)
	}
	result, err := store.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, "", err
	}
	objects := make([]Object, 0, len(result.Contents))
	for _, object := range result.Contents {
		objects = append(
			objects,
			Object{
				Key:      strings.TrimPrefix(aws.ToString(object.Key), prefix),
				Size:     aws.ToInt64(object.Size),
				Modified: aws.ToTime(object.LastModified),
				ETag:     aws.ToString(object.ETag),
			},
		)
	}
	return objects, aws.ToString(result.NextContinuationToken), nil
}

// Check проверяет доступность bucket и права чтения метаданных.
func (store *S3) Check(ctx context.Context) error {
	_, err := store.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(store.bucket)})
	return err
}

func notFound(err error) error {
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode() {
		case "NotFound", "NoSuchKey", "404":
			return fmt.Errorf("artifact: %w", os.ErrNotExist)
		}
	}
	return err
}

type reader struct {
	store  *S3
	ctx    context.Context
	key    string
	info   Object
	offset int64
	body   io.ReadCloser
}

func (reader *reader) Info() Object { return reader.info }
func (reader *reader) Close() error {
	if reader.body != nil {
		err := reader.body.Close()
		reader.body = nil
		return err
	}
	return nil
}

func (reader *reader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		offset += reader.offset
	case io.SeekEnd:
		offset += reader.info.Size
	case io.SeekStart:
	default:
		return 0, fmt.Errorf("invalid seek")
	}
	if offset < 0 {
		return 0, fmt.Errorf("negative seek")
	}
	if offset != reader.offset {
		if err := reader.Close(); err != nil {
			return 0, err
		}
		reader.offset = offset
	}
	return offset, nil
}

func (reader *reader) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	if reader.offset >= reader.info.Size {
		return 0, io.EOF
	}
	if reader.body == nil {
		result, err := reader.store.client.GetObject(
			reader.ctx,
			&s3.GetObjectInput{
				Bucket:  aws.String(reader.store.bucket),
				Key:     aws.String(reader.key),
				Range:   aws.String(fmt.Sprintf("bytes=%d-", reader.offset)),
				IfMatch: aws.String(reader.info.ETag),
			},
		)
		if err != nil {
			return 0, notFound(err)
		}
		reader.body = result.Body
	}
	count, err := reader.body.Read(buffer)
	reader.offset += int64(count)
	return count, err
}
