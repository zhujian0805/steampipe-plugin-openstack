package openstack

import (
	"context"

	"github.com/dihedron/steampipe-plugin-utils/utils"
	"github.com/gophercloud/gophercloud/openstack/loadbalancer/v2/pools"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

//// TABLE DEFINITION

func tableOpenStackPool(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "openstack_pool",
		Description: "OpenStack pool",
		Columns: []*plugin.Column{
			{
				Name:        "raw",
				Description: "Raw data.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromValue(),
			},
			{
				Name:        "id",
				Type:        proto.ColumnType_STRING,
				Description: "The unique id of the pool.",
				Transform:   transform.FromField("ID"),
			},
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "Human-readable name for the loadblancer.",
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "description",
				Type:        proto.ColumnType_STRING,
				Description: "The description of the project (or tenant)",
				Transform:   transform.FromField("Description"),
			},
			{
				Name:        "project_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the project owning this network.",
				Transform:   transform.FromField("ProjectID"),
			},
			{
				Name:        "protocol",
				Type:        proto.ColumnType_STRING,
				Description: "The protocol to loadbalance. A valid value is TCP, SCTP, HTTP, HTTPS or TERMINATED_HTTPS.",
				Transform:   transform.FromField("Protocol"),
			},
			{
				Name:        "subnet_id",
				Type:        proto.ColumnType_STRING,
				Description: "SubnetID",
				Transform:   transform.FromField("SubnetID"),
			},
			{
				Name:        "loadbalancers",
				Type:        proto.ColumnType_JSON,
				Description: "Loadbalancers",
				Transform:   transform.FromField("Loadbalancers"),
			},
			{
				Name:        "admin_state_up",
				Type:        proto.ColumnType_STRING,
				Description: "AdminStateUp",
				Transform:   transform.FromField("AdminStateUp"),
			},
			{
				Name:        "provisioning_status",
				Type:        proto.ColumnType_STRING,
				Description: "ProvisioningStatus",
				Transform:   transform.FromField("ProvisioningStatus"),
			},
			{
				Name:        "operating_status",
				Type:        proto.ColumnType_STRING,
				Description: "OperatingStatus",
				Transform:   transform.FromField("OperatingStatus"),
			},
			{
				Name:        "session_persistence",
				Type:        proto.ColumnType_JSON,
				Description: "Persistence",
				Transform:   transform.FromField("Persistence"),
			},
			{
				Name:        "listeners",
				Type:        proto.ColumnType_JSON,
				Description: "Listeners",
				Transform:   transform.FromField("Listeners"),
			},
			{
				Name:        "members",
				Type:        proto.ColumnType_JSON,
				Description: "Members",
				Transform:   transform.FromField("Members"),
			},
			{
				Name:        "tags",
				Type:        proto.ColumnType_JSON,
				Description: "Tags is a list of security group tags. Tags are arbitrarily defined strings attached to a security group.",
				Transform:   transform.FromField("Tags"),
			},
		},
		List: &plugin.ListConfig{
			Hydrate: listOpenStackpool,
			KeyColumns: plugin.KeyColumnSlice{
				&plugin.KeyColumn{
					Name:    "id",
					Require: plugin.Optional,
				},
				&plugin.KeyColumn{
					Name:    "name",
					Require: plugin.Optional,
				},
				&plugin.KeyColumn{
					Name:    "description",
					Require: plugin.Optional,
				},
				&plugin.KeyColumn{
					Name:    "project_id",
					Require: plugin.Optional,
				},
				&plugin.KeyColumn{
					Name:    "operating_status",
					Require: plugin.Optional,
				},
			},
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("id"),
			Hydrate:    getOpenStackpool,
		},
	}
}

//// LIST FUNCTION

func listOpenStackpool(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	setLogLevel(ctx, d)

	plugin.Logger(ctx).Debug("retrieving openstack pool list", "query data", utils.ToPrettyJSON(d))

	client, err := getServiceClient(ctx, d, LbaasV2)
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving client", "error", err)
		return nil, err
	}

	opts := buildOpenStackpoolFilter(ctx, d.EqualsQuals)

	allPages, err := pools.List(client, opts).AllPages()
	if err != nil {
		plugin.Logger(ctx).Error("error listing pool with options", "options", utils.ToPrettyJSON(opts), "error", err)
		return nil, err
	}
	allpools, err := pools.ExtractPools(allPages)
	plugin.Logger(ctx).Debug("retrieving openstack allpools", "query data", utils.ToPrettyJSON(allpools))
	if err != nil {
		plugin.Logger(ctx).Error("error extracting networks", "error", err)
		return nil, err
	}
	plugin.Logger(ctx).Debug("pools retrieved", "count", len(allpools))

	for _, pool := range allpools {
		if ctx.Err() != nil {
			plugin.Logger(ctx).Debug("context done, exit")
			break
		}
		pool := pool
		d.StreamListItem(ctx, &pool)
	}
	return nil, nil
}

//// HYDRATE FUNCTIONS

func getOpenStackpool(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	setLogLevel(ctx, d)

	id := d.EqualsQuals["id"].GetStringValue()
	plugin.Logger(ctx).Debug("retrieving openstack pool", "id", id)

	client, err := getServiceClient(ctx, d, LbaasV2)
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving client", "error", err)
		return nil, err
	}

	result := pools.Get(client, id)
	var pool *pools.Pool
	pool, err = result.Extract()
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving pool", "error", err)
		return nil, err
	}

	return pool, nil
}

func buildOpenStackpoolFilter(ctx context.Context, quals plugin.KeyColumnEqualsQualMap) pools.ListOpts {
	opts := pools.ListOpts{}

	if value, ok := quals["id"]; ok {
		opts.ID = value.GetStringValue()
	}
	if value, ok := quals["project_id"]; ok {
		opts.ProjectID = value.GetStringValue()
	}
	if value, ok := quals["name"]; ok {
		opts.Name = value.GetStringValue()
	}
	plugin.Logger(ctx).Debug("returning", "filter", utils.ToPrettyJSON(opts))
	return opts
}
