package openstack

import (
	"context"

	"github.com/dihedron/steampipe-plugin-utils/utils"
	"github.com/gophercloud/gophercloud/openstack/loadbalancer/v2/listeners"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

//// TABLE DEFINITION

func tableOpenStackListener(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "openstack_listener",
		Description: "OpenStack Listener",
		Columns: []*plugin.Column{
			{
				Name:        "id",
				Type:        proto.ColumnType_STRING,
				Description: "The unique id of the listener.",
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
				Name:        "default_pool_id",
				Type:        proto.ColumnType_STRING,
				Description: "DefaultPoolID",
				Transform:   transform.FromField("DefaultPoolID"),
			},
			{
				Name:        "loadbalancers",
				Type:        proto.ColumnType_JSON,
				Description: "Loadbalancers",
				Transform:   transform.FromField("Loadbalancers"),
			},
			{
				Name:        "default_pool",
				Type:        proto.ColumnType_JSON,
				Description: "DefaultPool",
				Transform:   transform.FromField("DefaultPool"),
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
				Description: "The operating status of the listener. This value is ONLINE or OFFLINE.",
				Transform:   transform.FromField("OperatingStatus"),
			},
			{
				Name:        "pools",
				Type:        proto.ColumnType_JSON,
				Description: "Pools",
				Transform:   transform.FromField("Pools"),
			},
			{
				Name:        "listeners",
				Type:        proto.ColumnType_JSON,
				Description: "Listeners are the listeners related to this listener.",
				Transform:   transform.FromField("Listeners"),
			},
			{
				Name:        "tags",
				Type:        proto.ColumnType_JSON,
				Description: "Tags is a list of security group tags. Tags are arbitrarily defined strings attached to a security group.",
				Transform:   transform.FromField("Tags"),
			},
		},
		List: &plugin.ListConfig{
			Hydrate: listOpenStacklistener,
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
			Hydrate:    getOpenStacklistener,
		},
	}
}

//// LIST FUNCTION

func listOpenStacklistener(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	setLogLevel(ctx, d)

	plugin.Logger(ctx).Debug("retrieving openstack listener list", "query data", utils.ToPrettyJSON(d))

	client, err := getServiceClient(ctx, d, LbaasV2)
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving client", "error", err)
		return nil, err
	}

	opts := buildOpenStacklistenerFilter(ctx, d.EqualsQuals)

	allPages, err := listeners.List(client, opts).AllPages()
	if err != nil {
		plugin.Logger(ctx).Error("error listing listener with options", "options", utils.ToPrettyJSON(opts), "error", err)
		return nil, err
	}
	alllisteners, err := listeners.ExtractListeners(allPages)
	if err != nil {
		plugin.Logger(ctx).Error("error extracting networks", "error", err)
		return nil, err
	}
	plugin.Logger(ctx).Debug("listeners retrieved", "count", len(alllisteners))

	for _, listener := range alllisteners {
		if ctx.Err() != nil {
			plugin.Logger(ctx).Debug("context done, exit")
			break
		}
		listener := listener
		d.StreamListItem(ctx, &listener)
	}
	return nil, nil
}

//// HYDRATE FUNCTIONS

func getOpenStacklistener(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	setLogLevel(ctx, d)

	id := d.EqualsQuals["id"].GetStringValue()
	plugin.Logger(ctx).Debug("retrieving openstack listener", "id", id)

	client, err := getServiceClient(ctx, d, LbaasV2)
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving client", "error", err)
		return nil, err
	}

	result := listeners.Get(client, id)
	var listener *listeners.Listener
	listener, err = result.Extract()
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving listener", "error", err)
		return nil, err
	}

	return listener, nil
}

func buildOpenStacklistenerFilter(ctx context.Context, quals plugin.KeyColumnEqualsQualMap) listeners.ListOpts {
	opts := listeners.ListOpts{}

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
