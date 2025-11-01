package spark_credential

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/dbt-labs/terraform-provider-dbtcloud/pkg/dbt_cloud"
	"github.com/dbt-labs/terraform-provider-dbtcloud/pkg/helper"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ resource.Resource                = &apacheSparkCredentialResource{}
	_ resource.ResourceWithConfigure   = &apacheSparkCredentialResource{}
	_ resource.ResourceWithImportState = &apacheSparkCredentialResource{}
)

func ApacheSparkCredentialResource() resource.Resource {
	return &apacheSparkCredentialResource{}
}

type apacheSparkCredentialResource struct {
	client *dbt_cloud.Client
}

func (a *apacheSparkCredentialResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	idParts := strings.Split(req.ID, ":")
	if len(idParts) != 2 {
		resp.Diagnostics.AddError(
			"Unexpected Import Identifier",
			fmt.Sprintf("Expected import identifier with format: project_id:credential_id. Got: %q", req.ID),
		)
		return
	}

	projectID, err := strconv.Atoi(idParts[0])
	if err != nil {
		resp.Diagnostics.AddError(
			"Unexpected Import Identifier",
			fmt.Sprintf("Could not convert project_id to integer. Got: %q", idParts[0]),
		)
		return
	}

	credentialID, err := strconv.Atoi(idParts[1])
	if err != nil {
		resp.Diagnostics.AddError(
			"Unexpected Import Identifier",
			fmt.Sprintf("Could not convert credential_id to integer. Got: %q", idParts[1]),
		)
		return
	}

	credentialResponse, err := a.client.GetApacheSparkCredential(projectID, credentialID)
	if err != nil {
		resp.Diagnostics.AddError("Error getting Apache Spark credential", err.Error())
		return
	}

	// Set ID
	resp.Diagnostics.Append(resp.State.SetAttribute(
		ctx,
		path.Root("id"),
		fmt.Sprintf("%d:%d", projectID, credentialID),
	)...)

	// Set project_id
	resp.Diagnostics.Append(resp.State.SetAttribute(
		ctx,
		path.Root("project_id"),
		projectID,
	)...)

	// Set credential_id
	resp.Diagnostics.Append(resp.State.SetAttribute(
		ctx,
		path.Root("credential_id"),
		credentialID,
	)...)

	// Set target_name
	resp.Diagnostics.Append(resp.State.SetAttribute(
		ctx,
		path.Root("target_name"),
		credentialResponse.Target_Name,
	)...)

	// Set schema from API response
	resp.Diagnostics.Append(resp.State.SetAttribute(
		ctx,
		path.Root("schema"),
		credentialResponse.UnencryptedCredentialDetails.Schema,
	)...)

	if resp.Diagnostics.HasError() {
		return
	}
}

func (a *apacheSparkCredentialResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*dbt_cloud.Client)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf(
				"Expected *dbt_cloud.Client, got: %T. Please report this issue to the provider developers.",
				req.ProviderData,
			),
		)
		return
	}

	a.client = client
}

func (a *apacheSparkCredentialResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan ApacheSparkCredentialResourceModel
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	a.createGlobal(ctx, &plan, resp)
}

func (a *apacheSparkCredentialResource) createGlobal(ctx context.Context, plan *ApacheSparkCredentialResourceModel, resp *resource.CreateResponse) {
	projectID := int(plan.ProjectID.ValueInt64())
	schema := plan.Schema.ValueString()
	targetName := plan.TargetName.ValueString()

	apacheSparkCredential, err := a.client.CreateApacheSparkCredential(
		projectID,
		schema,
		targetName,
	)
	if err != nil {
		resp.Diagnostics.AddError("Error creating Apache Spark credential", err.Error())
		return
	}

	plan.ID = types.StringValue(fmt.Sprintf("%d%s%d", apacheSparkCredential.Project_Id, dbt_cloud.ID_DELIMITER, *apacheSparkCredential.ID))
	plan.CredentialID = types.Int64Value(int64(*apacheSparkCredential.ID))

	diags := resp.State.Set(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (a *apacheSparkCredentialResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state ApacheSparkCredentialResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	a.deleteGlobal(ctx, &state, resp)
}

func (a *apacheSparkCredentialResource) deleteGlobal(_ context.Context, state *ApacheSparkCredentialResourceModel, resp *resource.DeleteResponse) {
	projectID := int(state.ProjectID.ValueInt64())
	credentialID := int(state.CredentialID.ValueInt64())

	_, err := a.client.DeleteCredential(
		strconv.Itoa(credentialID),
		strconv.Itoa(projectID),
	)
	if err != nil {
		// If the resource is already deleted (404), treat as success
		if strings.HasPrefix(err.Error(), "resource-not-found") {
			return
		}
		resp.Diagnostics.AddError("Error deleting Apache Spark credential", err.Error())
		return
	}
}

func (a *apacheSparkCredentialResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_apache_spark_credential"
}

func (a *apacheSparkCredentialResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state ApacheSparkCredentialResourceModel
	diags := req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	projectID := int(state.ProjectID.ValueInt64())
	credentialID := int(state.CredentialID.ValueInt64())

	credential, err := a.client.GetApacheSparkCredential(projectID, credentialID)
	if err != nil {
		resp.Diagnostics.AddError("Error reading Apache Spark credential", "Could not read Apache Spark credential ID "+state.ID.ValueString()+": "+err.Error())
		return
	}

	state.Schema = types.StringValue(credential.UnencryptedCredentialDetails.Schema)
	state.TargetName = types.StringValue(credential.UnencryptedCredentialDetails.TargetName)

	diags = resp.State.Set(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (a *apacheSparkCredentialResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = ApacheSparkResourceSchema
}

func (a *apacheSparkCredentialResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state ApacheSparkCredentialResourceModel
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	diags = req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	a.updateGlobal(ctx, &plan, &state, resp)
}

func (a *apacheSparkCredentialResource) updateGlobal(ctx context.Context, plan, state *ApacheSparkCredentialResourceModel, resp *resource.UpdateResponse) {
	projectID, credentialID, err := helper.SplitIDToInts(
		state.ID.ValueString(),
		"apache_spark_credential",
	)
	if err != nil {
		resp.Diagnostics.AddError("Invalid ID format", err.Error())
		return
	}

	// Check if any relevant fields have changed
	if !plan.TargetName.Equal(state.TargetName) ||
		!plan.Schema.Equal(state.Schema) {

		patchCredentialsDetails, err := dbt_cloud.GenerateApacheSparkCredentialDetails(
			plan.Schema.ValueString(),
			plan.TargetName.ValueString(),
		)
		if err != nil {
			resp.Diagnostics.AddError("Error generating credential details", err.Error())
			return
		}

		// Remove unchanged fields
		for key := range patchCredentialsDetails.Fields {
			switch key {
			case "schema":
				if plan.Schema.Equal(state.Schema) {
					delete(patchCredentialsDetails.Fields, key)
				}
			case "target_name":
				if plan.TargetName.Equal(state.TargetName) {
					delete(patchCredentialsDetails.Fields, key)
				}
			}
		}

		apacheSparkPatch := dbt_cloud.ApacheSparkCredentialGLobConnPatch{
			ID:                credentialID,
			CredentialDetails: patchCredentialsDetails,
		}

		_, err = a.client.UpdateApacheSparkCredentialGlobConn(projectID, credentialID, apacheSparkPatch)
		if err != nil {
			resp.Diagnostics.AddError("Error updating Apache Spark credential", err.Error())
			return
		}
	}

	diags := resp.State.Set(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
}
