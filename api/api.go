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
	apiv1 "k8s.io/api/core/v1"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type WorkerSpecs struct {
	WorkerId int `json:"workerId"`
	TotalWorkers int `json:"nWorkers"`
	InputFolderPath string `json:"inputFolderPath"`
}

type JobDetails struct {
	sync.Mutex
	JobName string
	InputFolderPath string
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
	http.HandleFunc("/status", jobStatus)
	http.HandleFunc("/init", initWorker)
	if err := http.ListenAndServe(":8000", nil); err != nil {
		panic(err)
	}
}

func generateUid() string {
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
		workers, _ := strconv.Atoi(r.PostForm.Get("workers"))
		uid := generateUid()
		jobName := "mapfunc-" + os.Getenv("COLUMBUS_USERNAME") + "-" + uid
		fmt.Printf("Created Job %s.\n", imageUrl)
		jobDetailsMap[uid] = &JobDetails{
			JobName: jobName,
			InputFolderPath: inputFolderPath,
			TotalWorkers: workers,
			RunningWorkers: 0,
			MapperUrl: "http://mapper-" + os.Getenv("COLUMBUS_USERNAME") + "/",
			uid: uid,
			imageUrl: imageUrl,
			completedWorkers: 0,
			accessToken: accessToken,
		}
		createJobHelper(jobDetailsMap[uid])
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

						},
					},
					RestartPolicy: apiv1.RestartPolicyNever,
				},
			},
			Completions: &nWorkers,
			BackoffLimit: &retries,
		},
	}
	result, err := jobsClient.Create(job)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Created Job %q.\n", result.GetObjectMeta().GetName())
}

func jobStatus(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	for i := 0; i<10; i++ {
		fmt.Fprintf(w, "data: %d\n\n", i)
		flusher.Flush()
		time.Sleep(2 * time.Second)
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
		if err := r.ParseMultiPartForm(3 << 30); err != nil {
			http.Error(w, "Could not parse form.", http.StatusBadRequest)
			return
		}
		chunk, handler, err := r.FormFile("chunk")
		uid := r.PostForm.Get("uid")
		if err := nil {
			panic(err)
		}
		defer chunk.close()
		chunkBytes, err := ioutil.ReadAll(chunk)

		filePath := "/storage/output/" + uid + ".csv"
		file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		defer file.close()
		file.Write(chunkBytes)
	}
}
