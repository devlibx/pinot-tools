package schema

import (
	"bytes"
	"encoding/json"
	"github.com/devlibx/gox-base/errors"
	"github.com/devlibx/gox-base/serialization"
	"github.com/go-resty/resty/v2"
	_ "github.com/go-resty/resty/v2"
	"os"
	"strings"
)

type tables struct {
	Tables []string `json:"tables"`
}

var OutDir = ""
var BaseUrl = ""
var DestinationBaseUrl = ""

func init() {
	BaseUrl = os.Getenv("PINOT_SRC_BASE_URL")
	if BaseUrl == "" {
		panic("base url is missing")
	}

	DestinationBaseUrl = os.Getenv("PINOT_DEST_BASE_URL")
	if DestinationBaseUrl == "" {
		DestinationBaseUrl = BaseUrl
	}

	OutDir = os.Getenv("OUT_DIR")
	if OutDir == "" {
		panic("outDir url is missing")
	}
}

func FetchSchemas() ([]string, error) {
	client := resty.New()
	resp, err := client.R().EnableTrace().Get(BaseUrl + "/schemas")
	if err != nil {
		return []string{}, errors.Wrap(err, "failed to fetch schemas")
	}

	schema := make([]string, 0)
	if err = json.Unmarshal(resp.Body(), &schema); err != nil {
		return []string{}, errors.Wrap(err, "failed to parse schema from response: response=%s", string(resp.Body()))
	}
	return schema, nil
}

func FetchSchema(schemaName string) (string, error) {
	client := resty.New()
	resp, err := client.R().EnableTrace().Get(BaseUrl + "/schemas/" + schemaName)
	if err != nil {
		return "", errors.Wrap(err, "failed to fetch schema: name="+schemaName)
	}
	return string(resp.Body()), nil
}

func FetchSchemaCurl(schemaName string) (string, error) {
	schema, err := FetchSchema(schemaName)
	if err != nil {
		return "", err
	}

	var prettyJSON bytes.Buffer
	if err = json.Indent(&prettyJSON, []byte(schema), "", "\t"); err == nil {
		schema = string(prettyJSON.Bytes())
	}

	curlStr := strings.ReplaceAll(schemaCurl, "__URL__", DestinationBaseUrl)
	curlStr = strings.ReplaceAll(curlStr, "__SCHEMA__", schema)
	return curlStr, nil
}

func FetchTables() ([]string, error) {
	client := resty.New()
	resp, err := client.R().EnableTrace().Get(BaseUrl + "/tables")
	if err != nil {
		return []string{}, errors.Wrap(err, "failed to fetch tables")
	}

	tables := tables{}
	if err = json.Unmarshal(resp.Body(), &tables); err != nil {
		return []string{}, errors.Wrap(err, "failed to parse tables from response: response=%s", string(resp.Body()))
	}
	return tables.Tables, nil
}

func FetchTable(tableName string) (map[string]interface{}, error) {
	client := resty.New()
	resp, err := client.R().EnableTrace().Get(BaseUrl + "/tables/" + tableName)
	if err != nil {
		return map[string]interface{}{}, errors.Wrap(err, "failed to fetch table: name="+tableName)
	}

	t := map[string]interface{}{}
	if err = json.Unmarshal(resp.Body(), &t); err != nil {
		return map[string]interface{}{}, errors.Wrap(err, "failed to parse table from response: response=%s", string(resp.Body()))
	}

	if m, ok := t["REALTIME"]; ok {
		return m.(map[string]interface{}), nil
	}
	return nil, errors.New("did not find table def for table=" + tableName)
}

func FetchTableCurl(table string) (string, error) {
	tableObj, err := FetchTable(table)
	if err != nil {
		return "", err
	}
	tableStr, err := serialization.Stringify(tableObj)
	if err != nil {
		return "", err
	}

	var prettyJSON bytes.Buffer
	if err = json.Indent(&prettyJSON, []byte(tableStr), "", "\t"); err == nil {
		tableStr = string(prettyJSON.Bytes())
	}

	curlStr := strings.ReplaceAll(tableCurl, "__URL__", DestinationBaseUrl)
	curlStr = strings.ReplaceAll(curlStr, "__TABLE__", serialization.StringifyOrDefaultOnError(tableStr, ""))
	return curlStr, nil
}

var schemaCurl = `
curl --location '__URL__/schemas?override=true' \
--header 'Content-Type: application/json' \
--data '__SCHEMA__'
`

var tableCurl = `
curl --location '__URL__/tables' \
--header 'Content-Type: application/json' \
--data '__TABLE__'
`
