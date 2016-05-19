all: controller

ARTIFACT=workflow-controller

# 0.0 shouldn't clobber any released builds
TAG = 0.0
#PREFIX = gcr.io/google_containers/${ARTIFACT}
PREFIX =  sdminonne/${ARTIFACT}

all: build

build:
	CGO_ENABLED=0 GOOS=linux godep go build -a -installsuffix cgo -ldflags '-w' -o ${ARTIFACT} ./${ARTIFACT}.go

container: build
	docker build -t $(PREFIX):$(TAG) .

push: container
	@echo "He we should push..."
	#docker push $(PREFIX):$(TAG)
	#gcloud docker push $(PREFIX):$(TAG)

clean:
	rm -f ${ARTIFACT}

.PHONY: build push clean
