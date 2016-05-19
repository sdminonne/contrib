#How to refresh your dependency

cd $GOPATH/src/k8s.io/contrib/workflow-controller
godep restore
go get -u k8s.io/kubernetes
cd $GOPATH/src/k8s.io/kubernetes
godep restore
cd $GOPATH/src/k8s.io/contrib/workflow-controller
rm -rf Godeps
godep save ./...
#git [add/remove] as needed
#git commit
