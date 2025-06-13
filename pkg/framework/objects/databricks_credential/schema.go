package databricks_credential

import (
	sl_cred_validator "github.com/dbt-labs/terraform-provider-dbtcloud/pkg/helper"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	datasource_schema "github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	resource_schema "github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
)

var dataSourceSchema = datasource_schema.Schema{
	Description: "Databricks credential data source",
	Attributes: map[string]datasource_schema.Attribute{
		"id": datasource_schema.StringAttribute{
			Description: "The ID of this resource. Contains the project ID and the credential ID.",
			Computed:    true,
		},
		"project_id": datasource_schema.Int64Attribute{
			Description: "Project ID",
			Required:    true,
		},
		"credential_id": datasource_schema.Int64Attribute{
			Description: "Credential ID",
			Required:    true,
		},
		"target_name": datasource_schema.StringAttribute{
			Description: "Target name",
			Computed:    true,
		},
		"num_threads": datasource_schema.Int64Attribute{
			Description: "The number of threads to use",
			Computed:    true,
		},
		"catalog": datasource_schema.StringAttribute{
			Description: "The catalog where to create models",
			Computed:    true,
		},
		"schema": datasource_schema.StringAttribute{
			Description: "The schema where to create models",
			Computed:    true,
		},
		"adapter_type": datasource_schema.StringAttribute{
			Description: "The type of the adapter (databricks or spark)",
			Computed:    true,
		},
	},
}

var DatabricksResourceSchema = resource_schema.Schema{
	Description: "Databricks credential resource",
	Attributes: map[string]resource_schema.Attribute{
		"id": resource_schema.StringAttribute{
			Description: "The ID of this resource. Contains the project ID and the credential ID.",
			Computed:    true,
			PlanModifiers: []planmodifier.String{
				stringplanmodifier.UseStateForUnknown(),
			},
		},
		"project_id": resource_schema.Int64Attribute{
			Description: "Project ID to create the Databricks credential in",
			Required:    true,
			PlanModifiers: []planmodifier.Int64{
				int64planmodifier.RequiresReplace(),
			},
		},
		"credential_id": resource_schema.Int64Attribute{
			Description: "The system Databricks credential ID",
			Computed:    true,
			PlanModifiers: []planmodifier.Int64{
				int64planmodifier.UseStateForUnknown(),
			},
		},
		"target_name": resource_schema.StringAttribute{
			Description:        "Target name",
			Optional:           true,
			Computed:           true,
			Default:            stringdefault.StaticString("default"),
			DeprecationMessage: "This field is deprecated at the environment level (it was never possible to set it in the UI) and will be removed in a future release. Please remove it and set the target name at the job level or leverage environment variables.",
		},
		"token": resource_schema.StringAttribute{
			Description: "Token for Databricks user",
			Required:    true,
			Sensitive:   true,
		},
		"catalog": resource_schema.StringAttribute{
			Description: "The catalog where to create models (only for the databricks adapter)",
			Optional:    true,
			Computed:    true,
			Default:     stringdefault.StaticString(""),
		},
		"schema": resource_schema.StringAttribute{
			Description: "The schema where to create models. Optional only when semantic_layer_credential is set to true; otherwise, this field is required.",
			Optional:    true,
			Computed:    true,
			Default:     stringdefault.StaticString("default_schema"),
			Validators: []validator.String{
				sl_cred_validator.SemanticLayerCredentialValidator{FieldName: "schema"},
			},
		},
		"adapter_type": resource_schema.StringAttribute{
			Description: "The type of the adapter (databricks or spark). Optional only when semantic_layer_credential is set to true; otherwise, this field is required.",
			Optional:    true,
			Computed:    true,
			Default:     stringdefault.StaticString("databricks"),
			PlanModifiers: []planmodifier.String{
				stringplanmodifier.RequiresReplace(),
			},
			Validators: []validator.String{
				stringvalidator.OneOf("databricks", "spark"),
				sl_cred_validator.SemanticLayerCredentialValidator{FieldName: "adapter_type"},
			},
		},
		"semantic_layer_credential": resource_schema.BoolAttribute{
			Optional:    true,
			Description: "This field indicates that the credential is used as part of the Semantic Layer configuration. It is used to create a Databricks credential for the Semantic Layer.",
			Computed:    true,
			Default:     booldefault.StaticBool(false),
		},
	},
}
