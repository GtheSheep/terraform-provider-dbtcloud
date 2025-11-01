package spark_credential

import (
	"github.com/hashicorp/terraform-plugin-framework/types"
)

type ApacheSparkCredentialDataSourceModel struct {
	ID           types.String `tfsdk:"id"`
	CredentialID types.Int64  `tfsdk:"credential_id"`
	ProjectID    types.Int64  `tfsdk:"project_id"`
	TargetName   types.String `tfsdk:"target_name"`
	NumThreads   types.Int64  `tfsdk:"num_threads"`
	Schema       types.String `tfsdk:"schema"`
}

type ApacheSparkCredentialResourceModel struct {
	ID           types.String `tfsdk:"id"`
	CredentialID types.Int64  `tfsdk:"credential_id"`
	ProjectID    types.Int64  `tfsdk:"project_id"`
	TargetName   types.String `tfsdk:"target_name"`
	Schema       types.String `tfsdk:"schema"`
}
