package spark_credential_test

import (
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/dbt-labs/terraform-provider-dbtcloud/pkg/dbt_cloud"
	"github.com/dbt-labs/terraform-provider-dbtcloud/pkg/framework/acctest_helper"
	"github.com/dbt-labs/terraform-provider-dbtcloud/pkg/framework/testhelpers"
	"github.com/dbt-labs/terraform-provider-dbtcloud/pkg/helper"
	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/stretchr/testify/assert"
)

func TestAccDbtCloudSparkCredentialResourceGlobConn(t *testing.T) {

	projectName := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))
	targetName := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))
	schema := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:                 func() { acctest_helper.TestAccPreCheck(t) },
		ProtoV6ProviderFactories: acctest_helper.TestAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckDbtCloudSparkCredentialDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccDbtCloudSparkCredentialResourceBasicConfigGlobConn(
					projectName,
					targetName,
					schema,
				),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckDbtCloudSparkCredentialExists(
						"dbtcloud_spark_credential.test_credential",
					),
					resource.TestCheckResourceAttr(
						"dbtcloud_spark_credential.test_credential",
						"catalog",
						catalog,
					),
				),
			},
			// ERROR schema must be provided
			{
				Config:      testCheckSchemaIsProvided(),
				ExpectError: regexp.MustCompile("`schema` must be provided when `semantic_layer_credential` is false."),
			},
			// RENAME
			// MODIFY
			{
				Config: testAccDbtCloudSparkCredentialResourceBasicConfigGlobConn(
					projectName,
					"",
					targetName,
					token2,
				),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckDbtCloudSparkCredentialExists(
						"dbtcloud_spark_credential.test_credential",
					),
					resource.TestCheckResourceAttr(
						"dbtcloud_spark_credential.test_credential",
						"catalog",
						"",
					),
					resource.TestCheckResourceAttr(
						"dbtcloud_spark_credential.test_credential",
						"token",
						token2,
					),
				),
			},
			// IMPORT
			{
				ResourceName:            "dbtcloud_spark_credential.test_credential",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"token", "adapter_type", "semantic_layer_credential"},
			},
		},
	})
}

func testAccDbtCloudSparkCredentialResourceBasicConfigGlobConn(
	projectName, targetName, schema string,
) string {
	return fmt.Sprintf(`
resource "dbtcloud_project" "test_project" {
  name        = "%s"
}

resource "dbtcloud_global_connection" "spark" {
  name = "My Spark connection"
  spark = {
    host      = "my-spark-host.cloud.spark.com"
    http_path = "/sql/my/http/path"
    catalog       = "dbt_catalog"
    client_id     = "yourclientid"
    client_secret = "yourclientsecret"
  }
}

resource "dbtcloud_environment" "prod_environment" {
  dbt_version     = "versionless"
  name            = "Prod"
  project_id      = dbtcloud_project.test_project.id
  connection_id   = dbtcloud_global_connection.spark.id
  type            = "deployment"
  credential_id   = dbtcloud_spark_credential.test_credential.credential_id
  deployment_type = "production"
}


resource "dbtcloud_spark_credential" "test_credential" {
    project_id = dbtcloud_project.test_project.id
    catalog = "%s"
	target_name = "%s"
    token   = "%s"
    schema  = "my_schema"
	adapter_type = "spark"
}
`, projectName, catalogName, targetName, token)
}

func testCheckSchemaIsProvided() string {
	return `
		resource "dbtcloud_project" "test_project" {
  			name        = "test"
		}

		resource "dbtcloud_spark_credential" "test_credential" {
    		project_id = dbtcloud_project.test_project.id
    		catalog = "test"
			target_name = "test"
    		token   = "test"
			adapter_type = "spark"
		}
	`
}

func testAccCheckDbtCloudSparkCredentialExists(resource string) resource.TestCheckFunc {
	return func(state *terraform.State) error {
		rs, ok := state.RootModule().Resources[resource]
		if !ok {
			return fmt.Errorf("Not found: %s", resource)
		}
		if rs.Primary.ID == "" {
			return fmt.Errorf("No Record ID is set")
		}
		projectId, credentialId, err := helper.SplitIDToInts(
			rs.Primary.ID,
			"dbtcloud_spark_credential",
		)
		if err != nil {
			return err
		}

		apiClient, err := acctest_helper.SharedClient()
		if err != nil {
			return fmt.Errorf("Issue getting the client")
		}
		_, err = apiClient.GetSparkCredential(projectId, credentialId)
		if err != nil {
			return fmt.Errorf("error fetching item with resource %s. %s", resource, err)
		}
		return nil
	}
}

func testAccCheckDbtCloudSparkCredentialDestroy(s *terraform.State) error {
	apiClient, err := acctest_helper.SharedClient()
	if err != nil {
		return fmt.Errorf("Issue getting the client")
	}

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "dbtcloud_spark_credential" {
			continue
		}
		projectId, credentialId, err := helper.SplitIDToInts(
			rs.Primary.ID,
			"dbtcloud_spark_credential",
		)
		if err != nil {
			return err
		}

		_, err = apiClient.GetSparkCredential(projectId, credentialId)
		if err == nil {
			return fmt.Errorf("Spark credential still exists")
		}
		notFoundErr := "resource-not-found"
		expectedErr := regexp.MustCompile(notFoundErr)
		if !expectedErr.Match([]byte(err.Error())) {
			return fmt.Errorf("expected %s, got %s", notFoundErr, err)
		}
	}

	return nil
}

func getBasicConfigTestStep(projectName, catalogName, targetName, token string) resource.TestStep {
	return resource.TestStep{
		Config: testAccDbtCloudSparkCredentialResourceBasicConfigGlobConn(
			projectName,
			catalogName,
			targetName,
			token,
		),
		Check: resource.ComposeTestCheckFunc(
			testAccCheckDbtCloudSparkCredentialExists(
				"dbtcloud_spark_credential.test_credential",
			),
			resource.TestCheckResourceAttr(
				"dbtcloud_spark_credential.test_credential",
				"target_name",
				targetName,
			),
		),
	}
}

func getModifyConfigTestStep(projectName, catalogName, targetName, targetName2, token, token2 string) resource.TestStep {
	return resource.TestStep{
		Config: testAccDbtCloudSparkCredentialResourceBasicConfigGlobConn(
			projectName,
			catalogName,
			targetName2,
			token2,
		),
		Check: resource.ComposeTestCheckFunc(
			testAccCheckDbtCloudSparkCredentialExists(
				"dbtcloud_spark_credential.test_credential",
			),
			resource.TestCheckResourceAttr(
				"dbtcloud_spark_credential.test_credential",
				"target_name",
				targetName2,
			),
			resource.TestCheckResourceAttr(
				"dbtcloud_spark_credential.test_credential",
				"token",
				token2,
			),
		),
	}
}

// Mock server utilities for bug fix regression tests

// TestSparkCredential_UpdateBugRegression tests the bug fix for credential updates
func TestSparkCredential_UpdateBugRegression(t *testing.T) {
	originalTFAcc := os.Getenv("TF_ACC")
	os.Setenv("TF_ACC", "1")
	defer func() {
		if originalTFAcc == "" {
			os.Unsetenv("TF_ACC")
		} else {
			os.Setenv("TF_ACC", originalTFAcc)
		}
	}()

	accountID, projectID, credentialID := 12345, 67890, 222
	tracker := &testhelpers.APICallTracker{}

	config := testhelpers.ResourceTestConfig{
		ResourceType: "dbtcloud_spark_credential",
		AccountID:    accountID,
		ProjectID:    projectID,
		ResourceID:   credentialID,
		APIPath:      "credentials",
	}

	handlers := testhelpers.CreateResourceTestHandlers(t, config, tracker)
	updateSparkCredentialHandlers(handlers, accountID, projectID, credentialID, tracker)

	srv := testhelpers.SetupMockServer(t, handlers)
	defer srv.Close()

	providerConfig := fmt.Sprintf(`
		provider "dbtcloud" {
			host_url   = "%s"
			token      = "dummy-token"
			account_id = %d
		}`, srv.URL, accountID)

	initialConfig := providerConfig + fmt.Sprintf(`
		resource "dbtcloud_spark_credential" "test" {
			project_id   = %d
			token        = "test_token"
			schema       = "test_schema"
			catalog      = "test_catalog"
			adapter_type = "spark"
		}`, projectID)

	updatedConfig := providerConfig + fmt.Sprintf(`
		resource "dbtcloud_spark_credential" "test" {
			project_id   = %d
			token        = "test_token"
			schema       = "updated_schema"
			catalog      = "test_catalog"
			adapter_type = "spark"
		}`, projectID)

	resource.Test(t, resource.TestCase{
		IsUnitTest:               true,
		ProtoV6ProviderFactories: acctest_helper.TestAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: initialConfig,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("dbtcloud_spark_credential.test", "schema", "test_schema"),
					resource.TestCheckResourceAttr("dbtcloud_spark_credential.test", "credential_id", fmt.Sprintf("%d", credentialID)),
				),
			},
			{
				Config: updatedConfig,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("dbtcloud_spark_credential.test", "schema", "updated_schema"),
					resource.TestCheckResourceAttr("dbtcloud_spark_credential.test", "credential_id", fmt.Sprintf("%d", credentialID)),
					verifySparkBugIsFixed(t, tracker),
				),
			},
		},
	})
}

func verifySparkBugIsFixed(t *testing.T, tracker *testhelpers.APICallTracker) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		assert.Equal(t, 1, tracker.CreateCount, "expected exactly 1 CREATE call")
		assert.Equal(t, 1, tracker.UpdateCount, "expected exactly 1 UPDATE call")
		assert.GreaterOrEqual(t, tracker.ReadCount, 1, "expected at least 1 READ call")
		return nil
	}
}

func updateSparkCredentialHandlers(handlers map[string]testhelpers.MockEndpointHandler, accountID, projectID, credentialID int, tracker *testhelpers.APICallTracker) {
	currentSchema := "test_schema"

	createResponse := func() dbt_cloud.ApacheSparkCredentialResponse {
		credentialDetails := dbt_cloud.AdapterCredentialDetails{
			Fields: map[string]dbt_cloud.AdapterCredentialField{
				"schema": {
					Value: currentSchema,
				},
				"catalog": {
					Value: "test_catalog",
				},
				"token": {
					Value: "test_token",
				},
			},
		}

		return dbt_cloud.ApacheSparkCredentialResponse{
			Data: dbt_cloud.ApacheSparkCredential{
				ID:                 &credentialID,
				Account_Id:         accountID,
				Project_Id:         projectID,
				Type:               "adapter",
				State:              1,
				Threads:            4,
				Target_Name:        "default",
				AdapterVersion:     "spark_v0",
				Credential_Details: credentialDetails,
				UnencryptedCredentialDetails: dbt_cloud.ApacheSparkUnencryptedCredentialDetails{
					Schema:     currentSchema,
					TargetName: "default",
					Threads:    4,
				},
			},
			Status: dbt_cloud.ResponseStatus{
				Code:         200,
				Is_Success:   true,
				User_Message: "",
			},
		}
	}

	createPath := fmt.Sprintf("POST /v3/accounts/%d/projects/%d/credentials/", accountID, projectID)
	handlers[createPath] = func(r *http.Request) (int, interface{}, error) {
		tracker.CreateCount++
		response := createResponse()
		response.Status.Code = 201
		return http.StatusCreated, response, nil
	}

	readPath := fmt.Sprintf("GET /v3/accounts/%d/projects/%d/credentials/%d/", accountID, projectID, credentialID)
	handlers[readPath] = func(r *http.Request) (int, interface{}, error) {
		tracker.ReadCount++
		return http.StatusOK, createResponse(), nil
	}

	updatePath := fmt.Sprintf("PATCH /v3/accounts/%d/projects/%d/credentials/%d/", accountID, projectID, credentialID)
	handlers[updatePath] = func(r *http.Request) (int, interface{}, error) {
		tracker.UpdateCount++
		currentSchema = "updated_schema"
		return http.StatusOK, createResponse(), nil
	}
}
