package tests

import (
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"

	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/pkg/constant"
)

type (
	IssueManagerService interface {
		GetIssue(issueKey string) *Issue
		ListTests(issueKey string) []*TestResult
		CreateTest(testCase *TestCase) (string, error)
		DeleteTest(testKey string) error
		CreateTestCycle(cycle *TestCycle) (string, error)
		DeleteTestCycle(cycleKey string) error
		CreateFolder(folder string) error
	}
	Jira struct {
		ProjectKey string
		UserName   string
		Password   string
		Url        string
		client     *resty.Client
	}
	CreateResult struct {
		Key string `json:"key"`
	}
	GetIssueFieldsStatus struct {
		Name string `json:"name"`
	}
	GetIssueFieldsParent struct {
		Key string `json:"key"`
	}
	GetIssueFields struct {
		Status   GetIssueFieldsStatus  `json:"status"`
		Parent   *GetIssueFieldsParent `json:"parent"`
		EpicName string                `json:"customfield_10001"`
	}
	GetIssueResult struct {
		Fields GetIssueFields `json:"fields"`
	}
)

func (j *Jira) GetClient() *resty.Client {
	client := j.client
	if client == nil {
		client = j.initClient()
		j.client = client
	}
	return client
}

func (j *Jira) initClient() *resty.Client {
	client := resty.New()
	fmt.Println(j.UserName)
	fmt.Println(j.Password)
	client.SetBasicAuth(j.UserName, j.Password)
	client.SetTimeout(constant.Ten * time.Second)
	return client
}

func (j *Jira) GetIssue(issueKey string) *Issue {
	url := j.Url + ""
	url = fmt.Sprintf("%s/rest/api/2/issue/%s?fields=status,parent,customfield_10001", url, issueKey)
	client := j.GetClient()
	res, err := client.R().SetResult(&GetIssueResult{}).Get(url)
	fmt.Println("GetIssue results:")
	fmt.Println(res)
	if err != nil {
		fmt.Println("There is error when GetIssue from Jira:")
		fmt.Println(err)
	}

	var issueRawFields GetIssueFields
	if (res != nil) && (err == nil) {
		issueRawFields = res.Result().(*GetIssueResult).Fields
	}
	issue := &Issue{
		Key:    issueKey,
		Status: issueRawFields.Status.Name,
		Epic:   issueRawFields.EpicName,
	}
	if issueRawFields.Parent != nil {
		parent := j.GetIssue(issueRawFields.Parent.Key)
		issue.Epic = parent.Epic
	}
	return issue
}

func (j *Jira) ListTests(issueKey string) []*TestResult {
	url := j.Url + "/rest/atm/1.0/testcase/search"
	client := j.GetClient()
	query := fmt.Sprintf("projectKey = \"%s\" AND issueKeys IN (%s)", j.ProjectKey, issueKey)
	fmt.Printf("url: %s?query=%s", url, query)
	res, err := client.R().SetQueryParam("query", query).SetResult([]*TestResult{}).Get(url)
	if err != nil {
		fmt.Println("Get testcases error: ")
		fmt.Println(err)
	}
	if (res != nil) && (err == nil) {
		return *res.Result().(*[]*TestResult)
	}
	return nil
}

func (j *Jira) CreateTest(testCase *TestCase) (string, error) {
	url := j.Url + "/rest/atm/1.0/testcase"
	testCase.ProjectKey = j.ProjectKey
	client := j.GetClient()
	resp, err := client.R().
		SetBody(testCase).
		SetResult(&CreateResult{}).
		Post(url)
	if err != nil {
		return "", err
	}
	return resp.Result().(*CreateResult).Key, nil
}

func (j *Jira) DeleteTest(testKey string) error {
	url := j.Url + "/rest/atm/1.0/testcase"
	url = fmt.Sprintf("%s/%s", url, testKey)
	client := j.GetClient()
	_, err := client.R().Delete(url)
	return err
}

func (j *Jira) CreateTestCycle(cycle *TestCycle) (string, error) {
	url := j.Url + "/rest/atm/1.0/testrun"
	cycle.ProjectKey = j.ProjectKey
	client := j.GetClient()
	resp, err := client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(cycle).
		SetResult(&CreateResult{}).
		Post(url)
	if err != nil {
		return "", err
	}
	return resp.Result().(*CreateResult).Key, nil
}

func (j *Jira) DeleteTestCycle(cycleKey string) error {
	url := j.Url + "/rest/atm/1.0/testrun"
	url = fmt.Sprintf("%s/%s", url, cycleKey)
	client := j.GetClient()
	_, err := client.R().Delete(url)
	return err
}

func (j *Jira) CreateFolder(folder string) error {
	url := j.Url + "/rest/atm/1.0/folder"
	client := j.GetClient()
	payload := &Folder{
		ProjectKey: j.ProjectKey,
		Name:       folder,
		Type:       "TEST_CASE",
	}
	_, err := client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(payload).
		SetResult(&CreateResult{}).
		Post(url)
	if err != nil {
		return err
	}
	payload.Type = "TEST_RUN"
	_, err = client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(payload).
		SetResult(&CreateResult{}).
		Post(url)
	return err
}
