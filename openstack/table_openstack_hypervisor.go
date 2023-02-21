package openstack

import (
	"context"

	"github.com/dihedron/steampipe-plugin-utils/utils"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/extensions/hypervisors"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

//// TABLE DEFINITION

func tableOpenStackHypervisor(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "openstack_hypervisor",
		Description: "OpenStack Hypervisor",
		Columns: []*plugin.Column{
			{
				Name:        "id",
				Type:        proto.ColumnType_STRING,
				Description: "The unique id of the hypervisor",
				Transform:   transform.FromField("ID"),
			},
			{
				Name:        "hypervisor_hostname",
				Type:        proto.ColumnType_STRING,
				Description: "The hostname of the hypervisor",
				Transform:   transform.FromField("HypervisorHostname"),
			},
			{
				Name:        "host_ip",
				Type:        proto.ColumnType_STRING,
				Description: "The host_ip of the hypervisor",
				Transform:   transform.FromField("HostIP"),
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "The state of the hypervisor",
				Transform:   transform.FromField("State"),
			},
			{
				Name:        "hypervisor_version",
				Type:        proto.ColumnType_STRING,
				Description: "The version of the hypervisor",
				Transform:   transform.FromField("HypervisorVersion"),
			},
			{
				Name:        "vcpus",
				Type:        proto.ColumnType_INT,
				Description: "The total number of vcpus on the hypervisor",
				Transform:   transform.FromField("VCPUs"),
			},
			{
				Name:        "cpu_vendor",
				Type:        proto.ColumnType_STRING,
				Description: "The vendor of the CPU",
				Transform:   transform.FromField("CPUInfo.Vendor"),
			},
			{
				Name:        "cpu_arch",
				Type:        proto.ColumnType_STRING,
				Description: "The arch of the CPU",
				Transform:   transform.FromField("CPUInfo.Arch"),
			},
			{
				Name:        "cpu_model",
				Type:        proto.ColumnType_STRING,
				Description: "The model of the CPU",
				Transform:   transform.FromField("CPUInfo.Model"),
			},
			{
				Name:        "vcpus_used",
				Type:        proto.ColumnType_INT,
				Description: "The number of vcpus used on the hypervisor",
				Transform:   transform.FromField("VCPUsUsed"),
			},
			{
				Name:        "current_workload",
				Type:        proto.ColumnType_INT,
				Description: "The current_workload of the hypervisor",
				Transform:   transform.FromField("CurrentWorkload"),
			},
			{
				Name:        "status",
				Type:        proto.ColumnType_STRING,
				Description: "The status of the hypervisor",
				Transform:   transform.FromField("Status"),
			},
			{
				Name:        "disk_available_least",
				Type:        proto.ColumnType_INT,
				Description: "The actual free disk on this hypervisor",
				Transform:   transform.FromField("DiskAvailableLeast"),
			},
			{
				Name:        "free_disk_on_hypervisor",
				Type:        proto.ColumnType_INT,
				Description: "the free disk remaining on the hypervisor, measured in GB",
				Transform:   transform.FromField("FreeDiskGB"),
			},
			{
				Name:        "free_ram_mb",
				Type:        proto.ColumnType_INT,
				Description: "The free RAM in the hypervisor, measured in MB",
				Transform:   transform.FromField("FreeRamMB"),
			},
			{
				Name:        "local_gb",
				Type:        proto.ColumnType_INT,
				Description: "the disk space in the hypervisor, measured in GB",
				Transform:   transform.FromField("LocalGB"),
			},
			{
				Name:        "local_gb_used",
				Type:        proto.ColumnType_INT,
				Description: "The used disk space of the hypervisor",
				Transform:   transform.FromField("LocalGBUsed"),
			},
			{
				Name:        "running_vms",
				Type:        proto.ColumnType_INT,
				Description: "The number of running vms on the hypervisor",
				Transform:   transform.FromField("RunningVMs"),
			},
			{
				Name:        "memory_mb",
				Type:        proto.ColumnType_INT,
				Description: "The total memory of the hypervisor",
				Transform:   transform.FromField("MemoryMB"),
			},
			{
				Name:        "memory_mb_used",
				Type:        proto.ColumnType_INT,
				Description: "The used memory of the hypervisor, measured in MB",
				Transform:   transform.FromField("MemoryMBUsed"),
			},
			{
				Name:        "service",
				Type:        proto.ColumnType_STRING,
				Description: "The service this hypervisor represents",
				Transform:   transform.FromField("Service"),
			},
			{
				Name:        "hypervisor_type",
				Type:        proto.ColumnType_STRING,
				Description: "The type of hypervisor",
				Transform:   transform.FromField("HypervisorType"),
			},
		},
		List: &plugin.ListConfig{
			Hydrate: listOpenStackHypervisor,
			KeyColumns: plugin.KeyColumnSlice{
				&plugin.KeyColumn{
					Name:    "id",
					Require: plugin.Optional,
				},
			},
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("id"),
			Hydrate:    getOpenStackHypervisor,
		},
	}
}

//// LIST FUNCTION

func listOpenStackHypervisor(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	setLogLevel(ctx, d)

	plugin.Logger(ctx).Debug("retrieving openstack hypervisor list", "query data", utils.ToPrettyJSON(d))

	client, err := getServiceClient(ctx, d, ComputeV2)
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving client", "error", err)
		return nil, err
	}

	opts := buildOpenStackHypervisorFilter(ctx, d.EqualsQuals)

	allPages, err := hypervisors.List(client, opts).AllPages()
	if err != nil {
		plugin.Logger(ctx).Error("error listing hypervisors with options", "options", utils.ToPrettyJSON(opts), "error", err)
		return nil, err
	}
	allHypervisors, err := hypervisors.ExtractHypervisors(allPages)
	plugin.Logger(ctx).Debug("all hypervisor", "all_hypervisor", utils.ToPrettyJSON(allHypervisors))
	if err != nil {
		plugin.Logger(ctx).Error("error extracting hypervisors", "error", err)
		return nil, err
	}
	plugin.Logger(ctx).Debug("hypervisors retrieved", "count", len(allHypervisors))

	for _, hypervisor := range allHypervisors {
		if ctx.Err() != nil {
			plugin.Logger(ctx).Debug("context done, exit")
			break
		}
		hypervisor := hypervisor
		plugin.Logger(ctx).Debug("hypervisor", "hypervisor", utils.ToPrettyJSON(hypervisor))
		d.StreamListItem(ctx, &hypervisor)
	}
	return nil, nil
}

//// HYDRATE FUNCTIONS

func getOpenStackHypervisor(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	setLogLevel(ctx, d)

	id := d.EqualsQuals["id"].GetStringValue()
	plugin.Logger(ctx).Debug("retrieving openstack hypervisor", "id", id)

	client, err := getServiceClient(ctx, d, ComputeV2)
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving client", "error", err)
		return nil, err
	}

	result := hypervisors.Get(client, id)
	var hypervisor *hypervisors.Hypervisor
	hypervisor, err = result.Extract()
	if err != nil {
		plugin.Logger(ctx).Error("error retrieving hypervisor", "error", err)
		return nil, err
	}

	return hypervisor, nil
}

func buildOpenStackHypervisorFilter(ctx context.Context, quals plugin.KeyColumnEqualsQualMap) hypervisors.ListOpts {

	opts := hypervisors.ListOpts{}

	plugin.Logger(ctx).Debug("returning", "filter", utils.ToPrettyJSON(opts))
	return opts
}
