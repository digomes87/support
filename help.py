def get_partition_filter(database_name, table_name, glue_client):
    table_partitions = []
    table_info = glue_client.get_table(
        DatabaseName=database_name,
        Name=table_name,
    )["Table"]
    partition_info = glue_client.get_partitions(
        DatabaseName=database_name, TableName=table_name, ExcludeColumnSchema=True
    )

    while True:
        if len(partition_info["Partitions"]) != 0:
            for partition in partition_info["Partitions"]:
                table_partitions.append(
                    {
                        "Values": partition["Values"],
                        "CreationTime": partition["CreationTime"],
                    }
                )

        next_token = partition_info.get("NextToken")
        if not next_token:
            break

        partition_info = glue_client.get_partitions(
            DatabaseName=database_name,
            TableName=table_name,
            ExcludeColumnSchema=True,
            NextToken=partition_info["NextToken"],
        )

    partitions_filter = ""
    table_partitions_name = table_info["PartitionKeys"]

    if len(table_partitions) == 0:
        return partitions_filter

    map_index = {}

    for index, creation_time in enumerate(table_partitions):
        map_index[index] = creation_time["CreationTime"]

    # Find the partition with the latest creation time
    max_partition = max(map_index, key=lambda x: map_index[x])
    partition = table_partitions[max_partition]["Values"]

    for index2, p in enumerate(table_partitions_name):
        name_partition = p["Name"]
        value_partition = partition[index2].strip()
        if index2 > 0 and partitions_filter != "":
            partitions_filter += " and "

        partitions_filter += f"{name_partition}={value_partition}"

    return partitions_filter
