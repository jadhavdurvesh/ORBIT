package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jadhavdurvesh/ORBIT/internal/model"
)

type client struct{ base string; http *http.Client }
func(c *client)post(path string,input,output any)error{
	var body bytes.Buffer
	if input!=nil{if err:=json.NewEncoder(&body).Encode(input);err!=nil{return err}}
	req,err:=http.NewRequest(http.MethodPost,c.base+path,&body);if err!=nil{return err}
	req.Header.Set("content-type","application/json")
	res,err:=c.http.Do(req);if err!=nil{return err};defer res.Body.Close()
	if res.StatusCode==http.StatusNoContent{return errNoJob}
	if res.StatusCode<200||res.StatusCode>=300{return fmt.Errorf("control plane returned %s",res.Status)}
	if output!=nil{return json.NewDecoder(res.Body).Decode(output)}
	return nil
}
var errNoJob=fmt.Errorf("no job available")

func execute(job model.Job)(map[string]string,error){
	switch job.Kind{
	case "echo":return map[string]string{"message":job.Payload["message"]},nil
	case "uppercase":return map[string]string{"value":strings.ToUpper(job.Payload["value"])},nil
	case "reverse":
		r:=[]rune(job.Payload["value"])
		for i,j:=0,len(r)-1;i<j;i,j=i+1,j-1{r[i],r[j]=r[j],r[i]}
		return map[string]string{"value":string(r)},nil
	case "sleep":
		d,err:=time.ParseDuration(job.Payload["duration"])
		if err!=nil{return nil,fmt.Errorf("sleep duration must be a Go duration: %w",err)}
		if d<0||d>20*time.Second{return nil,fmt.Errorf("sleep duration must be between 0 and 20s")}
		time.Sleep(d);return map[string]string{"slept":d.String()},nil
	default:return nil,fmt.Errorf("unsupported job kind %q",job.Kind)
	}
}

func main(){
	base:=os.Getenv("ORBIT_URL");if base==""{base="http://127.0.0.1:8080"};base=strings.TrimRight(base,"/")
	id:=os.Getenv("ORBIT_WORKER_ID")
	if id==""{id,_=os.Hostname()}
	if id==""{id="orbit-worker"}
	c:=&client{base:base,http:&http.Client{Timeout:10*time.Second}}
	worker:=map[string]any{"id":id,"capacity":1,"labels":[]string{"go","builtin"}}
	if err:=c.post("/v1/workers",worker,nil);err!=nil{log.Fatalf("register worker: %v",err)}
	log.Printf("worker %s connected to %s",id,base)

	heartbeat:=time.NewTicker(20*time.Second)
	defer heartbeat.Stop()
	go func(){
		for range heartbeat.C{
			if err:=c.post("/v1/workers/"+id+"/heartbeat",map[string]any{},nil);err!=nil{log.Printf("heartbeat: %v",err)}
		}
	}()

	poll:=time.NewTicker(500*time.Millisecond)
	defer poll.Stop()
	for range poll.C{
		var job model.Job
		err:=c.post("/v1/workers/"+id+"/claim",map[string]any{},&job)
		if err==errNoJob{continue}
		if err!=nil{log.Printf("claim: %v",err);continue}
		result,runErr:=execute(job)
		completion:=map[string]any{"worker_id":id,"success":runErr==nil,"result":result}
		if runErr!=nil{completion["error"]=runErr.Error()}
		if err:=c.post("/v1/jobs/"+job.ID+"/complete",completion,nil);err!=nil{log.Printf("complete %s: %v",job.ID,err)}
	}
}
