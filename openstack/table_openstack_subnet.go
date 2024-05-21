package openstack

import (
	"context"

	"github.com/dihedron/steampipe-plugin-utils/utils"
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack/networking/v2/subnets"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

//// TABLE DEFINITION

func tableOpenStackSubnet(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "openstack_subnet",
		Description: "OpenStack Subnet",
		Columns: []*plugin.Column{
			{
				Name:        "id",
				Type:        proto.ColumnType_STRING,
				Description: "The unique id of the subnet",
				Transform:   transform.FromField("ID"),
			},
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "Human-readable name for the subnet",
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "description",
				Type:        proto.ColumnType_STRING,
				Description: "The description of the subnet",
				Transform:   transform.FromField("Description"),
			},
			{
				Name:        "network",
				Type:        proto.ColumnType_STRING,
				Description: "The network the subnet belongs to",
				Transform:   transform.FromField("NetworkID"),
			},
			{
				Name:        "cidr",
				Type:        proto.ColumnType_STRING,
				Description: "The subnet cidr",
				Transform:   transform.FromField("CIDR"),
			},
			{
				Name:        "project_id",
				Type:        proto.ColumnType_STRING,
				Description: "The project id the subnet belongs to",
				Transform:   transform.FromField("ProjectID"),
			},
			{
				Name:        "dhcp",
				Type:        proto.ColumnType_BOOL,
				Description: "If DHCP is enabled",
				Transform:   transform.FromField("EnableDHCP"),
			},
			{
				Name:        "gateway",
				Type:        proto.ColumnType_STRING,
				Description: "The gateway of the subnet",
				Transform:   transform.FromField("GatewayIP"),
			},
			{
				Name:        "dns_nameservers",
				Type:        proto.ColumnType_JSON,
				Description: "DNS name servers used by hosts in this subnet.",
				Transform:   transform.FromField("DNSNameservers"),
			},
			{
				Name:        "host_routes",
				Type:        proto.ColumnType_JSON,
				Description: "Routes that should be used by devices with IPs from this subnet",
				Transform:   transform.FromField("HostRoutes"),
			},
		},
		List: &plugin.ListConfig{
			Hydrate: listOpenStackSubnet,
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
			},
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("id"),
			Hydrate:    getOpenStackSubnet,
		},
	}
}

//// LIST FUNCTION

func listOpenStackSubnet(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	setLogLevel(ctx, d)

	plugin.Logger(ctx).Debug("retrieving openstack subnet list", "query data", utils.ToPrettyJSON(d))

	client, err := getServiceClient(ctx, d, NetworkV2)
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving client", "error", err)
		return nil, err
	}

	opts := buildOpenStackSubnetFilter(ctx, d.EqualsQuals)

	allPages, err := subnets.List(client, opts).AllPages()
	if err != nil {
		plugin.Logger(ctx).Error("error listing subnets with options", "options", utils.ToPrettyJSON(opts), "error", err)
		return nil, err
	}
	allSubnets, err := subnets.ExtractSubnets(allPages)
	plugin.Logger(ctx).Debug("all subnet", "all_subnet", utils.ToPrettyJSON(allSubnets))
	if err != nil {
		plugin.Logger(ctx).Error("error extracting subnets", "error", err)
		return nil, err
	}
	plugin.Logger(ctx).Debug("subnets retrieved", "count", len(allSubnets))

	for _, subnet := range allSubnets {
		if ctx.Err() != nil {
			plugin.Logger(ctx).Debug("context done, exit")
			break
		}
		subnet := subnet
		plugin.Logger(ctx).Debug("subnet", "subnet", utils.ToPrettyJSON(subnet))
		d.StreamListItem(ctx, &subnet)
	}
	return nil, nil
}

//// HYDRATE FUNCTIONS

func getOpenStackSubnet(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	setLogLevel(ctx, d)

	id := d.EqualsQuals["id"].GetStringValue()
	plugin.Logger(ctx).Debug("retrieving openstack network", "id", id)

	client, err := getServiceClient(ctx, d, NetworkV2)
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving client", "error", err)
		return nil, err
	}

	result := subnets.Get(client, id)
	var subnet *subnets.Subnet
	subnet, err = result.Extract()
	if err != nil {
		_, ok1 := err.(gophercloud.ErrDefault404)
		_, ok2 := err.(gophercloud.ErrResourceNotFound)

		if ok1 || ok2 {
			plugin.Logger(ctx).Error("No resource found", "error", err)
			return nil, nil
		}
		plugin.Logger(ctx).Error("error retrieving network", "error", err)
		return nil, err
	}

	return subnet, nil
}

func buildOpenStackSubnetFilter(ctx context.Context, quals plugin.KeyColumnEqualsQualMap) subnets.ListOpts {

	opts := subnets.ListOpts{}

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

	plugin.Logger(ctx).Debug("returning", "filter", utils.ToPrettyJSON(opts))
	return opts
}
