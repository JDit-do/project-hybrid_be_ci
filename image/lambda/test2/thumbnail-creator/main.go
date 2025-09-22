package main

// io 패키지를 임포트해야 합니다.
import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/cshum/vipsgen/vips"
)

// S3Event는 Lambda 트리거로부터 받는 이벤트 정보입니다.
type S3Event struct {
	S3Bucket string `json:"s3Bucket"`
	S3Key    string `json:"s3Key"`
}

// ConversionResult는 람다 함수의 실행 결과를 담는 구조체입니다.
type ConversionResult struct {
	Status      string `json:"status"` // e.g., "CONVERTED", "SKIPPED_ALREADY_AVIF"
	OriginalKey string `json:"originalKey"`
	NewKey      string `json:"newKey,omitempty"` // 변환된 경우에만 값이 채워집니다.
	Message     string `json:"message,omitempty"`
}

var s3Client *s3.Client

// init 함수는 Lambda 콜드 스타트 시 한 번만 실행됩니다.
// S3 클라이언트와 vips 라이브러리를 초기화합니다.
func init() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	s3Client = s3.NewFromConfig(cfg)
	vips.Startup(nil)
	log.Println("S3 client and vips initialized successfully")
}

func HandleRequest(ctx context.Context, event S3Event) (ConversionResult, error) {
	srcKey, err := url.QueryUnescape(event.S3Key)
	if err != nil {
		// Fatalf 대신 에러 반환
		return ConversionResult{}, fmt.Errorf("failed to decode S3 key: %w", err)
	}
	log.Printf("Processing image: bucket=%s, key=%s", event.S3Bucket, srcKey)

	// 1. S3에서 이미지 객체 다운로드
	s3Object, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &event.S3Bucket,
		Key:    &srcKey,
	})
	if err != nil {
		return ConversionResult{}, fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer s3Object.Body.Close()

	// [수정] 스트림을 메모리 버퍼로 읽기
	imageBuffer, err := io.ReadAll(s3Object.Body)
	if err != nil {
		return ConversionResult{}, fmt.Errorf("failed to read image from S3 stream: %w", err)
	}
	originalSize := int64(len(imageBuffer)) // ContentLength 대신 버퍼 크기 사용

	// [수정] 파일이 아닌 버퍼에서 이미지 로드
	image, err := vips.NewImageFromBuffer(imageBuffer, nil)
	if err != nil {
		return ConversionResult{}, fmt.Errorf("failed to process image with vips from buffer: %w", err)
	}
	defer image.Close() // 이미지 객체 메모리 해제

	format, err := image.GetString("vips-loader")
	if err != nil {
		// 오류가 발생해도 변환을 시도하도록 로그만 남기고 넘어갈 수 있습니다.
		log.Printf("Warning: failed to get image format metadata: %v", err)
	} else {
		log.Printf("Detected loader: %s", format)
		// 이미 AVIF 포맷인지 확인
		if strings.HasPrefix(format, "heifload") {
			msg := "Image is already in AVIF format. Skipping conversion."
			log.Println(msg)
			return ConversionResult{
				Status:      "SKIPPED_ALREADY_AVIF",
				OriginalKey: srcKey,
				Message:     msg,
			}, nil
		}
	}

	options := &vips.HeifsaveBufferOptions{
		Q:             50,
		Bitdepth:      10,
		Lossless:      false,
		SubsampleMode: vips.SubsampleAuto,
		//Effort:        5,
		Compression: vips.HeifCompressionAv1,
		Encoder:     vips.HeifEncoderSvt,
	}

	log.Printf("DEBUG: Preparing to export with options: %+v\n", options)

	avifBuffer, err := image.HeifsaveBuffer(options)
	if err != nil {
		// vips 에러를 함께 로깅하면 디버깅에 더 유용합니다.
		return ConversionResult{}, fmt.Errorf("failed to encode image to AVIF: vips_error: %s", err)
	}
	log.Printf("Successfully encoded to AVIF. Original size: %d bytes, New size: %d bytes", originalSize, len(avifBuffer))

	// 변수 선언을 추가합니다.
	avifBufferSize := int64(len(avifBuffer))

	newKey := replaceExtension(srcKey, ".avif")
	log.Printf("Uploading converted image to: bucket=%s, key=%s", event.S3Bucket, newKey)

	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(event.S3Bucket), // aws.String 헬퍼 사용
		Key:         aws.String(newKey),
		Body:        bytes.NewReader(avifBuffer),
		ContentType: aws.String("image/avif"), // aws.String 헬퍼 사용

		ContentLength: &avifBufferSize,

		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		return ConversionResult{}, fmt.Errorf("failed to upload AVIF image to S3: %w", err)
	}

	return ConversionResult{
		Status:      "CONVERTED",
		OriginalKey: srcKey,
		NewKey:      newKey,
	}, nil
}

func main() {
	lambda.Start(HandleRequest)
}

func replaceExtension(key, newExt string) string {
	ext := filepath.Ext(key)
	if ext == "" {
		return key + newExt
	}
	return key[0:len(key)-len(ext)] + newExt
}
