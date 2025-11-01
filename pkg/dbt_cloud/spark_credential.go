package dbt_cloud

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type ApacheSparkCredentialListResponse struct {
	Data   []ApacheSparkCredential `json:"data"`
	Status ResponseStatus          `json:"status"`
}

type ApacheSparkCredentialResponse struct {
	Data   ApacheSparkCredential `json:"data"`
	Status ResponseStatus        `json:"status"`
}

type ApacheSparkUnencryptedCredentialDetails struct {
	Schema     string `json:"schema"`
	TargetName string `json:"target_name"`
	Threads    int    `json:"threads"`
}

type ApacheSparkCredential struct {
	ID                           *int                                    `json:"id"`
	Account_Id                   int                                     `json:"account_id"`
	Project_Id                   int                                     `json:"project_id"`
	Type                         string                                  `json:"type"`
	State                        int                                     `json:"state"`
	Threads                      int                                     `json:"threads"`
	Target_Name                  string                                  `json:"target_name"`
	AdapterVersion               string                                  `json:"adapter_version,omitempty"`
	Credential_Details           AdapterCredentialDetails                `json:"credential_details"`
	UnencryptedCredentialDetails ApacheSparkUnencryptedCredentialDetails `json:"unencrypted_credential_details"`
}

type ApacheSparkCredentialGlobConn struct {
	ID                *int                     `json:"id"`
	AccountID         int                      `json:"account_id"`
	ProjectID         int                      `json:"project_id"`
	Type              string                   `json:"type"`
	State             int                      `json:"state"`
	Threads           int                      `json:"threads"`
	AdapterVersion    string                   `json:"adapter_version"`
	CredentialDetails AdapterCredentialDetails `json:"credential_details"`
}

type ApacheSparkCredentialGLobConnPatch struct {
	ID                int                      `json:"id"`
	CredentialDetails AdapterCredentialDetails `json:"credential_details"`
}

func (c *Client) GetApacheSparkCredential(
	projectId int,
	credentialId int,
) (*ApacheSparkCredential, error) {
	req, err := http.NewRequest(
		"GET",
		fmt.Sprintf(
			"%s/v3/accounts/%d/projects/%d/credentials/%d/?include_related=[adapter]",
			c.HostURL,
			c.AccountID,
			projectId,
			credentialId,
		),
		nil,
	)
	if err != nil {
		return nil, err
	}

	body, err := c.doRequestWithRetry(req)

	if err != nil {
		return nil, err
	}

	credentialResponse := ApacheSparkCredentialResponse{}
	err = json.Unmarshal(body, &credentialResponse)
	if err != nil {
		return nil, err
	}

	return &credentialResponse.Data, nil
}

func (c *Client) CreateApacheSparkCredential(
	projectId int,
	schema string,
	targetName string,

) (*ApacheSparkCredential, error) {

	credentialDetails, err := GenerateApacheSparkCredentialDetails(
		schema,
		targetName,
	)
	if err != nil {
		return nil, err
	}

	newApacheSparkCredential := ApacheSparkCredentialGlobConn{
		AccountID:         c.AccountID,
		ProjectID:         projectId,
		Type:              "adapter",
		AdapterVersion:    "apache_spark_v0",
		State:             STATE_ACTIVE,
		Threads:           NUM_THREADS_CREDENTIAL,
		CredentialDetails: credentialDetails,
	}

	newApacheSparkCredentialData, err := json.Marshal(newApacheSparkCredential)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(
		"POST",
		fmt.Sprintf(
			"%s/v3/accounts/%d/projects/%d/credentials/",
			c.HostURL,
			c.AccountID,
			projectId,
		),
		strings.NewReader(string(newApacheSparkCredentialData)),
	)
	if err != nil {
		return nil, err
	}

	body, err := c.doRequestWithRetry(req)
	if err != nil {
		return nil, err
	}

	apacheSparkCredentialResponse := ApacheSparkCredentialResponse{}
	err = json.Unmarshal(body, &apacheSparkCredentialResponse)
	if err != nil {
		return nil, err
	}

	return &apacheSparkCredentialResponse.Data, nil
}

func (c *Client) UpdateApacheSparkCredentialGlobConn(
	projectId int,
	credentialId int,
	apacheSparkCredential ApacheSparkCredentialGLobConnPatch,
) (*ApacheSparkCredential, error) {
	apacheSparkCredentialData, err := json.Marshal(apacheSparkCredential)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(
		"PATCH",
		fmt.Sprintf(
			"%s/v3/accounts/%d/projects/%d/credentials/%d/",
			c.HostURL,
			c.AccountID,
			projectId,
			credentialId,
		),
		strings.NewReader(string(apacheSparkCredentialData)),
	)
	if err != nil {
		return nil, err
	}

	body, err := c.doRequestWithRetry(req)
	if err != nil {
		return nil, err
	}

	apacheSparkCredentialResponse := ApacheSparkCredentialResponse{}
	err = json.Unmarshal(body, &apacheSparkCredentialResponse)
	if err != nil {
		return nil, err
	}

	return &apacheSparkCredentialResponse.Data, nil
}

func GenerateApacheSparkCredentialDetails(
	schema string,
	targetName string,

) (AdapterCredentialDetails, error) {
	// Apache Spark credentials use a minimal credential_details structure
	// The connection details (host, port, cluster, etc.) are stored in the global_connection
	// Based on the API example provided, credential_details can be empty for Apache Spark
	defaultConfig := `{
		"fields": {
	      "schema": {
	        "metadata": {
	          "label": "Schema",
	          "description": "User schema.",
	          "field_type": "text",
	          "encrypt": false,
	          "overrideable": false,
	          "validation": {
	            "required": true
	          }
	        },
	        "value": ""
	      },
	      "target_name": {
	        "metadata": {
	          "label": "Target Name",
	          "description": "",
	          "field_type": "text",
	          "encrypt": false,
	          "overrideable": false,
	          "validation": {
	            "required": false
	          }
	        },
	        "value": ""
	      }
	    }
		}
`
	// we load the raw JSON to make it easier to update if the schema changes in the future
	var apacheSparkCredentialDetailsDefault AdapterCredentialDetails
	err := json.Unmarshal([]byte(defaultConfig), &apacheSparkCredentialDetailsDefault)
	if err != nil {
		return apacheSparkCredentialDetailsDefault, err
	}

	fieldMapping := map[string]interface{}{
		"schema":      schema,
		"target_name": targetName,
	}

	apacheSparkCredentialFields := map[string]AdapterCredentialField{}
	for key, value := range apacheSparkCredentialDetailsDefault.Fields {
		value.Value = fieldMapping[key]
		apacheSparkCredentialFields[key] = value
	}

	credentialDetails := AdapterCredentialDetails{
		Fields:      apacheSparkCredentialFields,
		Field_Order: []string{},
	}
	return credentialDetails, nil
}
