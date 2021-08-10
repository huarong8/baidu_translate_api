package main

import (
	"github.com/gocql/gocql"
	//"os"
	"time"
	"strings"
	"io"
	"crypto/md5"
	"strconv"
	"math/rand"
	"encoding/json"
	"log"
	"fmt"
	"net/http"
	"context"
	"net/url"
	"flag"
	"io/ioutil"
)

var appId = ""
var secret = ""


func Md5(input string) string{
	w := md5.New()
	io.WriteString(w, input)
	md5str := fmt.Sprintf("%x", w.Sum(nil))
	return md5str
}

func nomalFileName(en_filename string) string{
	en_filename = strings.ReplaceAll(en_filename, "-", "_")
	en_filename = strings.ReplaceAll(en_filename, " ", "_")
	en_filename = strings.ToLower(en_filename)
	return en_filename
}


var session *gocql.Session
func init(){
	host := "localhost:9042"
	keySpace := "smart_creative"
	cluster := gocql.NewCluster(host)
	cluster.Keyspace = keySpace
	cluster.ProtoVersion = 4
	var err error
	session, err = cluster.CreateSession()
	//defer session.Close()
	if err != nil {
		log.Fatal(err)
	}
	scanner := session.Query("desc keyspaces").Iter().Scanner()
	for scanner.Next(){
		var table string
		var table1 string
		var table2 string

		err := scanner.Scan(&table, &table1, &table2)
		if err != nil{
			log.Fatal(err)
		}
		//log.Println("table : ", table, table1, table2)
	}
}

func main(){
	//defer session.Close()
	var query string
	flag.StringVar(&query, "q", "默认值", "query words")
	flag.Parse()
	video_dir_prefix := "./data/video/"
	files, _ := ioutil.ReadDir(video_dir_prefix)
	ctx := context.Background()
	//fmt.Printf("%#v\n", session)
	b := session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	rand.Seed(time.Now().UnixNano())
	d := rand.Intn(10000) + 10000
    for _, f := range files {
		//fmt.Println(f.Name())
		//origin_filename := strings.TrimSpace(f.Name())
		//en_filename := Translate(origin_filename)
		//en_filename = nomalFileName(en_filename)
		subFiles, _ := ioutil.ReadDir(video_dir_prefix + f.Name())
		for _, sf := range subFiles{
			//sf_origin_filename := strings.TrimSpace(sf.Name())
			//fmt.Println(sf_origin_filename)
			//sf_en_filename := Translate(sf_origin_filename)
			//sf_en_filename = nomalFileName(sf_en_filename)
			//err := os.Rename(video_dir_prefix + f.Name() + "/" + sf.Name(), video_dir_prefix + f.Name() + "/" + sf_en_filename)
			//log.Println(err)
			final_file := video_dir_prefix + f.Name() + "/" + sf.Name()
			fmt.Println(f.Name(), final_file, fmt.Sprintf("{'%s'}", f.Name()))
			b.Entries = append(b.Entries, gocql.BatchEntry{
				Stmt: "insert into meta_video ( name , create_time , file_path , id , tags) values (?, ?, ?, ?, ?)",
				Args: []interface{}{sf.Name(), time.Now().Truncate(time.Millisecond), final_file[7:], d, []string{f.Name()}},
				Idempotent: true,
			})
		}
    }
	err := session.ExecuteBatch(b)
	if err != nil{
		log.Fatal(err)
	}

}

func Translate(query string) string{
	translateUrl := "https://fanyi-api.baidu.com/api/trans/vip/translate"
	v := url.Values{}
	query = strings.TrimSpace(query)
	v.Set("q", query)
	v.Set("from", "zh")
	v.Set("to", "en")
	v.Set("appid", appId)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomInt := r.Intn(10000) + 10000
	salt := strconv.Itoa(randomInt)
	v.Set("salt", salt)

	signValue := appId + query + salt + secret
	sign := Md5(signValue)
	v.Set("sign", sign)

	body := ioutil.NopCloser(strings.NewReader(v.Encode()))
	request, err := http.NewRequest("POST", translateUrl, body)
	if err != nil{
		log.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded;param=value")
	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil{
		log.Fatal(err)
	}
	defer resp.Body.Close()
	content, err := ioutil.ReadAll(resp.Body)
	if err != nil{
		log.Fatal(err)
	}
	result := make(map[string]interface{})
	err = json.Unmarshal(content, &result)
	if err != nil{
		log.Fatal(err)
	}
	dst := result["trans_result"].([]interface{})[0].(map[string]interface{})["dst"]
	return dst.(string)
}
