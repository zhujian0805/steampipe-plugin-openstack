package openstack

import (
	"context"

	"github.com/dihedron/steampipe-plugin-utils/utils"
	"github.com/gophercloud/gophercloud/openstack/loadbalancer/v2/loadbalancers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

//// TABLE DEFINITION

func tableOpenStackLoadBalancer(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "openstack_loadbalancer",
		Description: "OpenStack Loadbalancer",
		Columns: []*plugin.Column{
			{
				Name:        "id",
				Type:        proto.ColumnType_STRING,
				Description: "The unique id of the loadbalancer.",
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
				Name:        "flavor_id",
				Type:        proto.ColumnType_STRING,
				Description: "The UUID of a flavor if set.",
				Transform:   transform.FromField("FlavorID"),
			},
			{
				Name:        "vip_network_id",
				Type:        proto.ColumnType_STRING,
				Description: "The UUID of the network on which to allocate the virtual IP for the Loadbalancer address.",
				Transform:   transform.FromField("VipNetworkID"),
			},
			{
				Name:        "vip_subnet_id",
				Type:        proto.ColumnType_STRING,
				Description: "The UUID of the subnet on which to allocate the virtual IP for the",
				Transform:   transform.FromField("VipSubnetID"),
			},
			{
				Name:        "vip_address",
				Type:        proto.ColumnType_STRING,
				Description: "The IP address of the Loadbalancer.",
				Transform:   transform.FromField("VipAddress"),
			},
			{
				Name:        "provider",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the provider.",
				Transform:   transform.FromField("Provider"),
			},
			{
				Name:        "operating_status",
				Type:        proto.ColumnType_STRING,
				Description: "The operating status of the LoadBalancer. This value is ONLINE or OFFLINE.",
				Transform:   transform.FromField("OperatingStatus"),
			},
			{
				Name:        "pools",
				Type:        proto.ColumnType_JSON,
				Description: "Pools are the pools related to this Loadbalancer.",
				Transform:   transform.FromField("Pools"),
			},
			{
				Name:        "listeners",
				Type:        proto.ColumnType_JSON,
				Description: "Listeners are the listeners related to this Loadbalancer.",
				Transform:   transform.FromField("Listeners"),
			},
			{
				Name:        "vip_port_id",
				Type:        proto.ColumnType_STRING,
				Description: "The id of the vip port.",
				Transform:   transform.FromField("VipPortID"),
			},
			{
				Name:        "tags",
				Type:        proto.ColumnType_JSON,
				Description: "Tags is a list of security group tags. Tags are arbitrarily defined strings attached to a security group.",
				Transform:   transform.FromField("Tags"),
			},
		},
		List: &plugin.ListConfig{
			Hydrate: listOpenStackLoadbalancer,
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
			Hydrate:    getOpenStackLoadbalancer,
		},
	}
}

//// LIST FUNCTION

func listOpenStackLoadbalancer(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	setLogLevel(ctx, d)

	plugin.Logger(ctx).Debug("retrieving openstack loadbalancer list", "query data", utils.ToPrettyJSON(d))

	client, err := getServiceClient(ctx, d, LbaasV2)
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving client", "error", err)
		return nil, err
	}

	opts := buildOpenStackLoadbalancerFilter(ctx, d.EqualsQuals)

	allPages, err := loadbalancers.List(client, opts).AllPages()
	if err != nil {
		plugin.Logger(ctx).Error("error listing loadbalancer with options", "options", utils.ToPrettyJSON(opts), "error", err)
		return nil, err
	}
	allLoadbalancers, err := loadbalancers.ExtractLoadBalancers(allPages)
	if err != nil {
		plugin.Logger(ctx).Error("error extracting networks", "error", err)
		return nil, err
	}
	plugin.Logger(ctx).Debug("loadbalancers retrieved", "count", len(allLoadbalancers))

	for _, loadbalancer := range allLoadbalancers {
		if ctx.Err() != nil {
			plugin.Logger(ctx).Debug("context done, exit")
			break
		}
		loadbalancer := loadbalancer
		d.StreamListItem(ctx, &loadbalancer)
	}
	return nil, nil
}

//// HYDRATE FUNCTIONS

func getOpenStackLoadbalancer(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	setLogLevel(ctx, d)

	id := d.EqualsQuals["id"].GetStringValue()
	plugin.Logger(ctx).Debug("retrieving openstack loadbalancer", "id", id)

	client, err := getServiceClient(ctx, d, LbaasV2)
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving client", "error", err)
		return nil, err
	}

	result := loadbalancers.Get(client, id)
	var loadbalancer *loadbalancers.LoadBalancer
	loadbalancer, err = result.Extract()
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving loadbalancer", "error", err)
		return nil, err
	}

	return loadbalancer, nil
}

func buildOpenStackLoadbalancerFilter(ctx context.Context, quals plugin.KeyColumnEqualsQualMap) loadbalancers.ListOpts {
	opts := loadbalancers.ListOpts{}

	if value, ok := quals["id"]; ok {
		opts.ID = value.GetStringValue()
	}
	if value, ok := quals["project_id"]; ok {
		opts.ProjectID = value.GetStringValue()
	}
	if value, ok := quals["name"]; ok {
		opts.Name = value.GetStringValue()
	}
	if value, ok := quals["description"]; ok {
		opts.Description = value.GetStringValue()
	}
	if value, ok := quals["operating_status"]; ok {
		opts.OperatingStatus = value.GetStringValue()
	}

	plugin.Logger(ctx).Debug("returning", "filter", utils.ToPrettyJSON(opts))
	return opts
}
