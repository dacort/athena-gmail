.PHONY: docker test

# Environment variables
AWS_REGION?=us-east-1
AWS_ACCOUNT_ID?=YOUR_ACCOUNT_ID
REPOSITORY_ID=${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
CONTAINER_LABEL?=gathena
LAMBDA_FUNCTION_NAME?=gathena_container

IMAGE_DIGEST=$(shell docker images --no-trunc --digests --format '{{.Digest}}' ${REPOSITORY_ID}/${CONTAINER_LABEL} | head -n 1)

docker:
	aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${REPOSITORY_ID}
	docker build -t ${CONTAINER_LABEL} .
	docker tag ${CONTAINER_LABEL}:latest ${REPOSITORY_ID}/${CONTAINER_LABEL}:latest
	docker push ${REPOSITORY_ID}/${CONTAINER_LABEL}:latest
	# aws lambda update-function-code --function-name ${LAMBDA_FUNCTION_NAME} --image-uri "${REPOSITORY_ID}/${CONTAINER_LABEL}@$(IMAGE_DIGEST)" --region ${AWS_REGION}

test:
	python -m pytest test
