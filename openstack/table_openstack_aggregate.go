package openstack

import (
	"context"

	"github.com/dihedron/steampipe-plugin-utils/utils"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/extensions/aggregates"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

//// TABLE DEFINITION

func tableOpenStackAggregate(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "openstack_aggregate",
		Description: "OpenStack Aggregate",
		Columns: []*plugin.Column{
			{
				Name:        "id",
				Type:        proto.ColumnType_STRING,
				Description: "The unique id of the aggregate",
				Transform:   transform.FromField("ID"),
			},
			{
				Name:        "hosts",
				Type:        proto.ColumnType_JSON,
				Description: "A list of host ids in this aggregate",
				Transform:   transform.FromField("Hosts"),
			},
			{
				Name:        "availability_zone",
				Type:        proto.ColumnType_STRING,
				Description: "The availability zone of the host aggregate",
				Transform:   transform.FromField("AvailabilityZone"),
			},
			{
				Name:        "metadata",
				Type:        proto.ColumnType_JSON,
				Description: "Metadata key and value pairs associate with the aggregate",
				Transform:   transform.FromField("Metadata"),
			},
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "Name of the aggregate",
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "created_at",
				Type:        proto.ColumnType_JSON,
				Description: "The date and time when the resource was created",
				Transform:   transform.FromField("CreatedAt"),
			},
			{
				Name:        "updated_at",
				Type:        proto.ColumnType_JSON,
				Description: "The date and time when the resource was updated",
				Transform:   transform.FromField("UpdatedAt"),
			},
		},
		List: &plugin.ListConfig{
			Hydrate: listOpenStackAggregate,
			KeyColumns: plugin.KeyColumnSlice{
				&plugin.KeyColumn{
					Name:    "id",
					Require: plugin.Optional,
				},
			},
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("id"),
			Hydrate:    getOpenStackAggregate,
		},
	}
}

//// LIST FUNCTION

func listOpenStackAggregate(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	setLogLevel(ctx, d)

	plugin.Logger(ctx).Debug("retrieving openstack aggregate list", "query data", utils.ToPrettyJSON(d))

	client, err := getServiceClient(ctx, d, ComputeV2)
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving client", "error", err)
		return nil, err
	}

	allPages, err := aggregates.List(client).AllPages()
	if err != nil {
		plugin.Logger(ctx).Error("error listing aggregates with options", "options", utils.ToPrettyJSON(allPages), "error", err)
		return nil, err
	}
	allAggregates, err := aggregates.ExtractAggregates(allPages)
	plugin.Logger(ctx).Debug("all aggregate", "all_aggregate", utils.ToPrettyJSON(allAggregates))
	if err != nil {
		plugin.Logger(ctx).Error("error extracting aggregates", "error", err)
		return nil, err
	}
	plugin.Logger(ctx).Debug("aggregates retrieved", "count", len(allAggregates))

	for _, aggregate := range allAggregates {
		if ctx.Err() != nil {
			plugin.Logger(ctx).Debug("context done, exit")
			break
		}
		aggregate := aggregate
		plugin.Logger(ctx).Debug("aggregate", "aggregate", utils.ToPrettyJSON(aggregate))
		d.StreamListItem(ctx, &aggregate)
	}
	return nil, nil
}

//// HYDRATE FUNCTIONS

func getOpenStackAggregate(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	setLogLevel(ctx, d)

	id := d.EqualsQuals["id"].GetInt64Value()
	plugin.Logger(ctx).Debug("retrieving openstack aggregate", "id", id)

	client, err := getServiceClient(ctx, d, ComputeV2)
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving client", "error", err)
		return nil, err
	}

	result := aggregates.Get(client, int(id))
	var aggregate *aggregates.Aggregate
	aggregate, err = result.Extract()
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving aggregate", "error", err)
		return nil, err
	}

	return aggregate, nil
}
