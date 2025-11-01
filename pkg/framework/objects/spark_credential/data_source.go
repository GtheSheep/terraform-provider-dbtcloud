package spark_credential

import (
	"context"
	"fmt"

	"github.com/dbt-labs/terraform-provider-dbtcloud/pkg/dbt_cloud"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ datasource.DataSource              = &apacheSparkCredentialDataSource{}
	_ datasource.DataSourceWithConfigure = &apacheSparkCredentialDataSource{}
)

func ApacheSparkCredentialDataSource() datasource.DataSource {
	return &apacheSparkCredentialDataSource{}
}

type apacheSparkCredentialDataSource struct {
	client *dbt_cloud.Client
}

func (a *apacheSparkCredentialDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*dbt_cloud.Client)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Data Source Configure Type",
			fmt.Sprintf(
				"Expected *dbt_cloud.Client, got: %T. Please report this issue to the provider developers.",
				req.ProviderData,
			),
		)
		return
	}

	a.client = client
}

func (a *apacheSparkCredentialDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_apache_spark_credential"
}

func (a *apacheSparkCredentialDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config ApacheSparkCredentialDataSourceModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	projectID := int(config.ProjectID.ValueInt64())
	credentialID := int(config.CredentialID.ValueInt64())

	apacheSparkCredential, err := a.client.GetApacheSparkCredential(projectID, credentialID)
	if err != nil {
		resp.Diagnostics.AddError("Error getting Apache Spark credential", err.Error())
		return
	}

	config.ID = types.StringValue(fmt.Sprintf("%d:%d", projectID, *apacheSparkCredential.ID))
	config.ProjectID = types.Int64Value(int64(apacheSparkCredential.Project_Id))
	config.CredentialID = types.Int64Value(int64(*apacheSparkCredential.ID))
	config.TargetName = types.StringValue(apacheSparkCredential.Target_Name)
	config.NumThreads = types.Int64Value(int64(apacheSparkCredential.Threads))
	config.Schema = types.StringValue(apacheSparkCredential.UnencryptedCredentialDetails.Schema)

	diags := resp.State.Set(ctx, &config)
	resp.Diagnostics.Append(diags...)
}

func (a *apacheSparkCredentialDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = dataSourceSchema
}
