package tests

import (
	ctx "context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"go.tekoapis.com/kitchen/log/level"

	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/pkg/helper"
)

const JiraBaseTestFolder = "/HN7/API/SC"
const JiraProjectKey = "ERP2020"

type (
	TestScript struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	TestCase struct {
		Name       string     `json:"name"`
		ProjectKey string     `json:"projectKey"`
		IssueLinks []string   `json:"issueLinks"`
		Objective  string     `json:"objective"`
		Folder     string     `json:"folder"`
		TestScript TestScript `json:"testScript"`
		Status     string     `json:"status"`
	}
	TestCycleItem struct {
		TestCaseKey string `json:"testCaseKey"`
		Status      string `json:"status"`
	}
	Folder struct {
		ProjectKey string `json:"projectKey"`
		Name       string `json:"name"`
		Type       string `json:"type"`
	}
	Issue struct {
		Key    string
		Status string
		Epic   string
	}
	TestResult struct {
		Key      string `json:"key"`
		Name     string `json:"name"`
		Status   string `json:"status"`
		CreateBy string `json:"createdBy"`
	}
	TestCycle struct {
		Name             string           `json:"name"`
		ProjectKey       string           `json:"projectKey"`
		IssueKey         string           `json:"issueKey"`
		PlannedStartDate string           `json:"plannedStartDate"`
		PlannedEndDate   string           `json:"plannedEndDate"`
		Folder           string           `json:"folder"`
		Items            []*TestCycleItem `json:"items"`
	}

	TestData struct {
		mu          sync.RWMutex
		tests       map[string][]*TestCase
		cycleTests  map[string]string
		issueManSrv IssueManagerService
	}
)

var instance *TestData

func GetTest() *TestData {
	if instance == nil {
		instance = new(TestData)
		instance.tests = make(map[string][]*TestCase)
		instance.cycleTests = make(map[string]string)
		instance.issueManSrv = LoadDefaultConfig()
	}
	return instance
}

func (t *TestData) removeOldTests(issueKey string, tests []*TestCase) {
	newTests, exist := t.tests[issueKey]
	if !exist {
		newTests = []*TestCase{}
	} else {
		// Remove old test cycle and tests
		oldTestCycle := t.cycleTests[issueKey]
		err := t.issueManSrv.DeleteTestCycle(oldTestCycle)
		if err != nil {
			level.Error(ctx.Background()).F("error when delete test cycle: %#v", err)
		}
	}
	// Only support all tests of an issue in only one package
	// So we delete all old tests of this issue then recreate
	oldTests := t.issueManSrv.ListTests(issueKey)
	fmt.Printf("removeOldTestCase of issueKey %s : number of removing case is %d", issueKey, len(oldTests))
	for _, oldItem := range oldTests {
		err := t.issueManSrv.DeleteTest(oldItem.Key)
		if err != nil {
			level.Error(ctx.Background()).F("error when delete tests: %#v", err)
		}
	}
	newTests = append(newTests, tests...)
	t.tests[issueKey] = newTests
}

func (t *TestData) PushTests(issueKey string, apiName string, testFolder string, tests []*TestCase) {
	if t.issueManSrv == (*Jira)(nil) {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	issue := t.issueManSrv.GetIssue(issueKey)
	if strings.ToLower(issue.Status) == "closed" || strings.ToLower(issue.Status) == "done" {
		return
	}
	if os.Getenv("remove_old_test") != "" {
		t.removeOldTests(issueKey, tests)
	} else {
		t.tests[issueKey] = tests
	}
	cycleItems := make([]*TestCycleItem, 0, len(t.tests[issueKey]))
	createdFolders := map[string]bool{}
	status := "Draft"
	//if strings.ToUpper(issue.Status) == "OPEN" ||
	//	strings.ToUpper(issue.Status) == "READY FOR DEV" ||
	//	strings.ToUpper(issue.Status) == "API FUNCTIONAL TEST CASE REVIEW" ||
	//	strings.ToUpper(issue.Status) == "WRITE API FUNCTIONAL TEST" ||
	//	strings.ToUpper(issue.Status) == "TO DO" {
	//	status = "Draft"
	//}
	folder := fmt.Sprintf("%s/%s", JiraBaseTestFolder, testFolder)
	if _, ok := createdFolders[folder]; !ok {
		if err := t.issueManSrv.CreateFolder(folder); err != nil {
			level.Error(ctx.Background()).F("error when create folder: %#v", err)
		}
		createdFolders[folder] = true
	}
	for _, item := range t.tests[issueKey] {

		testCase := &TestCase{
			Name:       item.Name,
			IssueLinks: []string{issueKey},
			Folder:     folder,
			ProjectKey: JiraProjectKey,
			Objective:  "",
			TestScript: TestScript{
				Type: "PLAIN_TEXT",
				Text: "",
			},
			Status: status,
		}
		testKey, err := t.issueManSrv.CreateTest(testCase)
		if err != nil {
			level.Error(ctx.Background()).F("error when push tests: %#v", err)
		}
		cycleItems = append(cycleItems, &TestCycleItem{
			TestCaseKey: testKey,
			Status:      item.Status,
		})
	}
	now := time.Now().Format("2006-01-02T15:04:05")
	cycle := &TestCycle{
		Name:             fmt.Sprintf("[%s][%s]%s", issue.Epic, issue.Key, apiName),
		IssueKey:         issueKey,
		Folder:           folder,
		ProjectKey:       JiraProjectKey,
		PlannedStartDate: now,
		PlannedEndDate:   now,
		Items:            cycleItems,
	}
	cycleKey, err := t.issueManSrv.CreateTestCycle(cycle)
	if err != nil {
		level.Error(ctx.Background()).F("error when push tests: %#v", err)
	}
	t.cycleTests[issueKey] = cycleKey
}

func LoadDefaultConfig() *Jira {
	var jira *Jira
	path := helper.UnitTestFile
	var config *os.File
	var err error
	for i := 0; i < 5; i++ {
		config, err = helper.OpenFile(path)
		if err != nil {
			path = "../" + path
			continue
		}
		break
	}
	if err != nil || config == nil {
		fmt.Println("Error: Cannot find unittest config file, testcases won't be pushed to jira")
		return nil
	}
	defer config.Close()
	decoder := json.NewDecoder(config)
	if err = decoder.Decode(&jira); err != nil {
		fmt.Println("Error: Cannot parse unittest config file, testcases won't be pushed to jira")
		return nil
	}
	return jira
}

func RootDir() string {
	_, b, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	d := path.Join(path.Dir(b))
	return filepath.Dir(d)
}
