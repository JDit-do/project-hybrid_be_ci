docker buildx build --platform linux/arm64 --provenance=false -t image-name:latest .


테스트 방법
{
  "s3Bucket": "버킷이름",
  "s3Key": "이미지 경로"
}