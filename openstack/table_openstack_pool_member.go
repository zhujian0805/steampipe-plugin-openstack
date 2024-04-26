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

func tableOpenStackPoolMember(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "openstack_pool_member",
		Description: "OpenStack Pool Member",
		Columns: []*plugin.Column{
			{
				Name:        "id",
				Type:        proto.ColumnType_STRING,
				Description: "The unique id of the pool.",
				Transform:   transform.FromField("ID"),
			},
			{
				Name:        "pool_id",
				Type:        proto.ColumnType_STRING,
				Description: "PoolID",
				Transform:   transform.FromField("PoolID"),
			},
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "Human-readable name for the loadblancer.",
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "project_id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID of the project owning this network.",
				Transform:   transform.FromField("ProjectID"),
			},
			{
				Name:        "address",
				Type:        proto.ColumnType_STRING,
				Description: "Address",
				Transform:   transform.FromField("Address"),
			},
			{
				Name:        "subnet_id",
				Type:        proto.ColumnType_STRING,
				Description: "SubnetID",
				Transform:   transform.FromField("SubnetID"),
			},
			{
				Name:        "backup",
				Type:        proto.ColumnType_BOOL,
				Description: "Backup",
				Transform:   transform.FromField("Backup"),
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
				Name:        "protocol_port",
				Type:        proto.ColumnType_STRING,
				Description: "ProtocolPort",
				Transform:   transform.FromField("ProtocolPort"),
			},
			{
				Name:        "operating_status",
				Type:        proto.ColumnType_STRING,
				Description: "OperatingStatus",
				Transform:   transform.FromField("OperatingStatus"),
			},
			{
				Name:        "tags",
				Type:        proto.ColumnType_JSON,
				Description: "Tags is a list of security group tags. Tags are arbitrarily defined strings attached to a security group.",
				Transform:   transform.FromField("Tags"),
			},
		},
		List: &plugin.ListConfig{
			Hydrate: listOpenStackPoolMember,
			KeyColumns: plugin.KeyColumnSlice{
				&plugin.KeyColumn{
					Name:    "pool_id",
					Require: plugin.Optional,
				},
				&plugin.KeyColumn{
					Name:    "id",
					Require: plugin.Optional,
				},
			},
		},
	}
}

func listOpenStackPoolMember(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	setLogLevel(ctx, d)

	plugin.Logger(ctx).Debug("returning", "filter", utils.ToPrettyJSON(d))

	pool_id := d.EqualsQuals["pool_id"].GetStringValue()

	plugin.Logger(ctx).Debug("retrieving openstack pool member", "pool id", pool_id)

	client, err := getServiceClient(ctx, d, LbaasV2)

	if err != nil {
		plugin.Logger(ctx).Error("error retrieving client", "error", err)
		return nil, err
	}

	// opts := buildOpenStackPoolMemberFilter(ctx, d.EqualsQuals)
	opts := pools.ListMembersOpts{}

	allPages, err := pools.ListMembers(client, pool_id, opts).AllPages()

	plugin.Logger(ctx).Debug("allPages", "---->", utils.ToPrettyJSON(allPages))

	if err != nil {
		plugin.Logger(ctx).Error("error listing pool with options", "options", utils.ToPrettyJSON(opts), "error", err)
		return nil, err
	}

	allmembers, err := pools.ExtractMembers(allPages)

	if err != nil {
		plugin.Logger(ctx).Error("error extracting networks", "error", err)
		return nil, err
	}

	plugin.Logger(ctx).Debug("allPools", "---->", utils.ToPrettyJSON(allmembers))

	for _, member := range allmembers {
		if ctx.Err() != nil {
			plugin.Logger(ctx).Debug("context done, exit")
			break
		}
		member.PoolID = pool_id
		plugin.Logger(ctx).Debug("pool", "---->", utils.ToPrettyJSON(member))
		d.StreamListItem(ctx, member)
	}

	return nil, nil
}

// // HYDRATE FUNCTIONS
func buildOpenStackPoolFilter(ctx context.Context, quals plugin.KeyColumnEqualsQualMap) pools.ListOpts {
	opts := pools.ListOpts{}

	if value, ok := quals["id"]; ok {
		opts.ID = value.GetStringValue()
	}
	if value, ok := quals["name"]; ok {
		opts.Name = value.GetStringValue()
	}
	plugin.Logger(ctx).Debug("returning", "filter", utils.ToPrettyJSON(opts))
	return opts
}

func buildOpenStackPoolMemberFilter(ctx context.Context, quals plugin.KeyColumnEqualsQualMap) ListPoolMembersOpts {
	opts := ListPoolMembersOpts{}

	if value, ok := quals["id"]; ok {
		opts.ID = value.GetStringValue()
	}
	if value, ok := quals["name"]; ok {
		opts.Name = value.GetStringValue()
	}
	if value, ok := quals["project_id"]; ok {
		opts.ProjectID = value.GetStringValue()
	}
	if value, ok := quals["pool_id"]; ok {
		opts.PoolID = value.GetStringValue()
	}
	plugin.Logger(ctx).Debug("returning", "filter", utils.ToPrettyJSON(opts))
	return opts
}

type ListPoolMembersOpts struct {
	PoolID string
	pools.ListMembersOpts
}
