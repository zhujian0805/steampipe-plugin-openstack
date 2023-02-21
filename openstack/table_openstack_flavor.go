package openstack

import (
	"context"

	"github.com/dihedron/steampipe-plugin-utils/utils"
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/flavors"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

//// TABLE DEFINITION

func tableOpenStackFlavor(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "openstack_flavor",
		Description: "OpenStack Flavors",
		Columns: []*plugin.Column{
			{
				Name:        "id",
				Type:        proto.ColumnType_STRING,
				Description: "The unique id of the flavor",
				Transform:   transform.FromField("ID"),
			},
			{
				Name:        "disk",
				Type:        proto.ColumnType_INT,
				Description: "Disk is the amount of root disk, measured in GB",
				Transform:   transform.FromField("Disk"),
			},
			{
				Name:        "ram",
				Type:        proto.ColumnType_INT,
				Description: "RAM is the amount of memory, measured in MB",
				Transform:   transform.FromField("RAM"),
			},
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "Name is the name of the flavor",
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "vcpus",
				Type:        proto.ColumnType_INT,
				Description: "VCPUs indicates how many (virtual) CPUs are available for this flavor",
				Transform:   transform.FromField("VCPUs"),
			},
			{
				Name:        "is_public",
				Type:        proto.ColumnType_BOOL,
				Description: "IsPublic indicates whether the flavor is public",
				Transform:   transform.FromField("IsPublic"),
			},
			{
				Name:        "Description",
				Type:        proto.ColumnType_STRING,
				Description: "Description is a free form description of the flavor",
				Transform:   transform.FromField("Description"),
			},
		},
		List: &plugin.ListConfig{
			Hydrate: listOpenStackFlavor,
			KeyColumns: plugin.KeyColumnSlice{
				&plugin.KeyColumn{
					Name:    "id",
					Require: plugin.Optional,
				},
			},
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("id"),
			Hydrate:    getOpenStackFlavor,
		},
	}
}

//// LIST FUNCTION

func listOpenStackFlavor(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	setLogLevel(ctx, d)

	opts := buildOpenStackFlavorFilter(ctx, d.EqualsQuals)

	plugin.Logger(ctx).Debug("retrieving openstack flavor list", "query data", utils.ToPrettyJSON(d))

	client, err := getServiceClient(ctx, d, ComputeV2)
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving client", "error", err)
		return nil, err
	}

	allPages, err := flavors.ListDetail(client, opts).AllPages()

	if err != nil {
		return nil, err
	}

	if err != nil {
		plugin.Logger(ctx).Error("error listing flavors with options", "options", utils.ToPrettyJSON(allPages), "error", err)
		return nil, err
	}
	allFlavors, err := flavors.ExtractFlavors(allPages)
	plugin.Logger(ctx).Debug("all flavor", "all_flavor", utils.ToPrettyJSON(allFlavors))
	if err != nil {
		plugin.Logger(ctx).Error("error extracting flavors", "error", err)
		return nil, err
	}
	plugin.Logger(ctx).Debug("flavors retrieved", "count", len(allFlavors))

	for _, flavor := range allFlavors {
		if ctx.Err() != nil {
			plugin.Logger(ctx).Debug("context done, exit")
			break
		}
		flavor := flavor
		plugin.Logger(ctx).Debug("flavor", "flavor", utils.ToPrettyJSON(flavor))
		d.StreamListItem(ctx, &flavor)
	}
	return nil, nil
}

//// HYDRATE FUNCTIONS

func getOpenStackFlavor(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	setLogLevel(ctx, d)

	id := d.EqualsQuals["id"].GetStringValue()
	plugin.Logger(ctx).Debug("retrieving openstack flavor", "id", id)

	client, err := getServiceClient(ctx, d, ComputeV2)
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving client", "error", err)
		return nil, err
	}

	result := flavors.Get(client, id)
	var flavor *flavors.Flavor
	flavor, err = result.Extract()
	if err != nil {

		_, ok1 := err.(gophercloud.ErrDefault404)
		_, ok2 := err.(gophercloud.ErrResourceNotFound)

		if ok1 || ok2 {
			plugin.Logger(ctx).Error("No Resource Found", "error", err)
			return nil, nil
		}

		plugin.Logger(ctx).Error("error retrieving flavor", "error", err)
		return nil, err
	}

	return flavor, nil
}

func buildOpenStackFlavorFilter(ctx context.Context, quals plugin.KeyColumnEqualsQualMap) flavors.ListOpts {

	opts := flavors.ListOpts{
		ChangesSince: "",
		MinDisk:      0,
		MinRAM:       0,
		SortDir:      "",
		SortKey:      "",
		Marker:       "",
		Limit:        0,
		AccessType:   "None",
	}
	return opts
}
