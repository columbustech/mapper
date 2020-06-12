package main

import (
	"fmt"
	"net/http"
	"math/rand"
	"time"
	"strconv"
	"encoding/json"
	"strings"
	"os"
	"sync"
	"io/ioutil"
	"mime/multipart"
	"path/filepath"
	"bytes"
	"reflect"
	apiv1 "k8s.io/api/core/v1"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type WorkerSpecs struct {
	WorkerId int `json:"workerId"`
	TotalWorkers int `json:"nWorkers"`
	InputFolderPath string `json:"inputFolderPath"`
}

type UidSpec struct {
	Uid string `json:"uid"`
}

type Fields struct {
	Key string `json:"key"`
	XAmzAlgorithm string `json:"x-amz-algorithm"`
	XAmzCredential string `json:"x-amz-credential"`
	XAmzDate string `json:"x-amz-date"`
	Policy string `json:"policy"`
	XAmzSignature string `json:"x-amz-signature"`
}

type UploadSpec struct {
	Url string `json:"url"`
	Fields Fields `json:"fields"`
	UploadId string `json:"uploadId"`
}

type JobDetails struct {
	sync.Mutex
	JobName string
	InputFolderPath string
	OutputPath string
	OutputName string
	TotalWorkers int
	RunningWorkers int
	MapperUrl string
	uid string
	imageUrl string
	completedWorkers int
	accessToken string
}

var jobDetailsMap map[string]*JobDetails

func main() {
	_ = os.Mkdir("/storage/output", 0755)
	jobDetailsMap = make(map[string]*JobDetails)
	http.HandleFunc("/create", createJob)
	http.HandleFunc("/init", initWorker)
	http.HandleFunc("/status", jobStatus)
	http.HandleFunc("/write-chunk", writeChunk)
	http.HandleFunc("/delete", deleteJob)
	http.HandleFunc("/upload", uploadOutput)
	server := &http.Server{
		ReadTimeout: 1 * time.Minute,
		WriteTimeout: 10 * time.Minute,
		Addr:":8000",
	}
	server.ListenAndServe()
}

func generateUid() string {
	rand.Seed(time.Now().UnixNano())
	letters := []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	s := make([]rune, 10)
	for i:= range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func createJob(w http.ResponseWriter, r* http.Request) {
	if r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Could not parse form.", http.StatusBadRequest)
			return
		}
		authUrl := r.Header.Get("Authorization")
		tokens := strings.Split(authUrl, " ")
		accessToken := tokens[1]
		imageUrl := r.PostForm.Get("imageUrl")
		inputFolderPath := r.PostForm.Get("inputFolderPath")
		outputPath := r.PostForm.Get("outputPath")
		outputName := r.PostForm.Get("outputName")
		workers, _ := strconv.Atoi(r.PostForm.Get("workers"))
		uid := generateUid()
		jobName := "mapfunc-" + os.Getenv("COLUMBUS_USERNAME") + "-" + uid
		fmt.Printf("Created Job %s.\n", imageUrl)
		jobDetailsMap[uid] = &JobDetails{
			JobName: jobName,
			InputFolderPath: inputFolderPath,
			OutputPath: outputPath,
			OutputName: outputName,
			TotalWorkers: workers,
			RunningWorkers: 0,
			MapperUrl: "http://mapper-" + os.Getenv("COLUMBUS_USERNAME") + "/",
			uid: uid,
			imageUrl: imageUrl,
			completedWorkers: 0,
			accessToken: accessToken,
		}
		_ = os.Mkdir("/storage/output/" + uid, 0755)
		createJobHelper(jobDetailsMap[uid])
		uidSpec := &UidSpec{
			Uid: uid,
		}
		json.NewEncoder(w).Encode(uidSpec)
	}
}

func createJobHelper(jobDetails *JobDetails) {
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	nWorkers := int32(jobDetails.TotalWorkers)
	retries := int32(5)
	jobsClient := clientset.BatchV1().Jobs(apiv1.NamespaceDefault)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: jobDetails.JobName,
		},
		Spec: batchv1.JobSpec{
			Template: apiv1.PodTemplateSpec{
				Spec: apiv1.PodSpec {
					Containers: []apiv1.Container{
						{
							Name: jobDetails.JobName,
							Image: jobDetails.imageUrl,
							Args: []string{"-u", jobDetails.MapperUrl, "-i", jobDetails.uid},
							Env: []apiv1.EnvVar{
								{
									Name: "COLUMBUS_ACCESS_TOKEN",
									Value: jobDetails.accessToken,
								},
							},
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceCPU: resource.MustParse("1"),
								},
							},

						},
					},
					RestartPolicy: apiv1.RestartPolicyNever,
				},
			},
			Completions: &nWorkers,
			Parallelism: &nWorkers,
			BackoffLimit: &retries,
		},
	}
	result, err := jobsClient.Create(job)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Created Job %q.\n", result.GetObjectMeta().GetName())
	go watchJob(jobDetails)
}

func watchJob(jobDetails *JobDetails) {
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	labelSelector := fmt.Sprintf("job-name=%s", jobDetails.JobName)
	listOptions := metav1.ListOptions{
		LabelSelector: labelSelector,
	}
	watcher, err := clientset.BatchV1().Jobs(apiv1.NamespaceDefault).Watch(listOptions)
	if err != nil {
		panic(err)
	}
	ch := watcher.ResultChan()
	for event := range ch {
		jobObject := event.Object.(*batchv1.Job)
		if conditions := jobObject.Status.Conditions; len(conditions) > 0 && conditions[0].Type == "Complete" {
			uploadToCDrive("/storage/output/" + jobDetails.uid + "/" + jobDetails.OutputName, jobDetails.OutputPath, jobDetails.accessToken)
			break
		}
	}
}

func jobStatus(w http.ResponseWriter, r *http.Request) {
	uid := r.URL.Query().Get("uid")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	jobDetails := jobDetailsMap[uid]
	labelSelector := fmt.Sprintf("job-name=%s", jobDetails.JobName)
	listOptions := metav1.ListOptions{
		LabelSelector: labelSelector,
	}
	watcher, err := clientset.BatchV1().Jobs(apiv1.NamespaceDefault).Watch(listOptions)
	if err != nil {
		panic(err)
	}
	ch := watcher.ResultChan()
	for event := range ch {
		jobObject := event.Object.(*batchv1.Job)
		s, _ := json.Marshal(jobObject.Status)
		fmt.Fprintf(w, "%s\n", string(s))
		flusher.Flush()
		if conditions := jobObject.Status.Conditions; len(conditions) > 0 && conditions[0].Type == "Complete" {
			break
		}
	}
}

func initWorker(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Could not parse form.", http.StatusBadRequest)
			return
		}
		uid := r.PostForm.Get("uid")
		w.Header().Set("Content-Type", "application/json")
		jobDetails := jobDetailsMap[uid]
		jobDetails.Lock()
		workerSpecs := &WorkerSpecs {
			WorkerId: jobDetails.RunningWorkers,
			TotalWorkers: jobDetails.TotalWorkers,
			InputFolderPath: jobDetails.InputFolderPath,
		}
		jobDetails.RunningWorkers++
		jobDetails.Unlock()
		json.NewEncoder(w).Encode(workerSpecs)
	}
}

func writeChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		if err := r.ParseMultipartForm(3 << 30); err != nil {
			http.Error(w, "Could not parse form.", http.StatusBadRequest)
			return
		}
		chunk, _ , err := r.FormFile("chunk")
		uid := r.PostForm.Get("uid")
		if err != nil {
			panic(err)
		}
		defer chunk.Close()
		chunkBytes, err := ioutil.ReadAll(chunk)

		jobDetails := jobDetailsMap[uid]

		filePath := "/storage/output/" + uid + "/" + jobDetails.OutputName
		if _, err := os.Stat(filePath); err == nil {
			chunkBytes = chunkBytes[ bytes.IndexByte(chunkBytes, byte('\n')) + 1 : ]
		}
		file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		defer file.Close()
		file.Write(chunkBytes)
	}
}

func uploadOutput(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Could not parse form.", http.StatusBadRequest)
			return
		}
		authUrl := r.Header.Get("Authorization")
		tokens := strings.Split(authUrl, " ")
		accessToken := tokens[1]
		localPath := r.PostForm.Get("localPath")
		cdrivePath := r.PostForm.Get("cdrivePath")
		uploadToCDrive(localPath, cdrivePath, accessToken)
	}
}

func uploadToCDrive(localPath, cDrivePath, accessToken string) {
	uploadSpec := initiateUpload(localPath, cDrivePath, accessToken)
	presignedUpload(uploadSpec, localPath)
	completeUpload(uploadSpec, accessToken)
}

func initiateUpload(localPath, cDrivePath, accessToken string) *UploadSpec{
	url := "http://cdrive/initiate-upload-alt/"
	var jsonStr = []byte(fmt.Sprintf(`{"path":"%s/%s"}`, cDrivePath, filepath.Base(localPath)))
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer " + accessToken)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	uploadSpec := UploadSpec{}
	json.NewDecoder(resp.Body).Decode(&uploadSpec)
	resp.Body.Close()
	return &uploadSpec
}

func presignedUpload(uploadSpec *UploadSpec, localPath string) {
	url := uploadSpec.Url
	file, err := os.Open(localPath)
	if err != nil {
		panic(err)
	}
	fileContents, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}
	file.Close()

	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)

	val := reflect.ValueOf(uploadSpec.Fields)
	for i := 0; i < val.Type().NumField(); i++ {
		tag := val.Type().Field(i).Tag.Get("json")
		fieldVal := val.Field(i).Interface()
		_ = writer.WriteField(tag, fmt.Sprintf("%v", fieldVal))
	}
	part, err := writer.CreateFormFile("file", filepath.Base(file.Name()))
	if err != nil {
		panic(err)
	}
	part.Write(fileContents)
	writer.Close()
	request, err := http.NewRequest("POST", url, body)
	if err != nil {
		panic(err)
	}
	request.Header.Add("Content-Type", writer.FormDataContentType())
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	response, err := client.Do(request)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()
	_ , err = ioutil.ReadAll(response.Body)
	if err != nil {
		panic(err)
	}
}

func completeUpload(uploadSpec *UploadSpec, accessToken string) {
	url := "http://cdrive/complete-upload-alt/"
	var jsonStr = []byte(fmt.Sprintf(`{"uploadId":"%s"}`, uploadSpec.UploadId))
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer " + accessToken)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	resp.Body.Close()
}

func deleteJob(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Could not parse form.", http.StatusBadRequest)
			return
		}
		uid := r.PostForm.Get("uid")

		deletePolicy := metav1.DeletePropagationForeground

		config, err := rest.InClusterConfig()
		if err != nil {
			panic(err.Error())
		}

		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			panic(err.Error())
		}
		jobDetails := jobDetailsMap[uid]
		jobsClient := clientset.BatchV1().Jobs(apiv1.NamespaceDefault)
		_ = jobsClient.Delete(jobDetails.JobName, &metav1.DeleteOptions{PropagationPolicy: &deletePolicy})
		delete(jobDetailsMap, uid)
	}
}
