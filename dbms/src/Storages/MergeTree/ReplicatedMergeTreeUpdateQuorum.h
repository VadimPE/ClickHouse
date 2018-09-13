#pragma once
#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>


namespace DB
{

/// Checksum of one file.
struct ReplicatedMergeTreeUpdateQuorum
{
	using PartsIdToBlocks = std::unordered_map<String, Int64>;
	using PartsIdToPartNames = std::unordered_map<String, String>;

	PartsIdToPartNames added_parts;
	MergeTreeDataFormatVersion format_version;

	ReplicatedMergeTreeUpdateQuorum(const MergeTreeDataFormatVersion & format_version_)
	: format_version(format_version_) {}

	PartIdToBlock getBlocks()
	{
		MaxAddedBlocks max_added_blocks;

	    for (auto & part : added_parts)
	        max_added_blocks[part.first] = MergeTreePartInfo::fromPartName(part.second, format_version).max_block;

	    return max_added_blocks;
	}

	PartIdToBlock read(ReadBuffer & in)
	{
		if (checkString("parts_count\t", in))
			read_v2(in);
		else
			read_v3(in);

		return getBlocks();
	}


	void read_v2(ReadBuffer & in);
	{
		size_t count;
		readText(count, in);
		assertChar('\n', in);

		for (size_t i = 0; i < count; ++i)
		{
			std::string part_id;
			std::string part_name;

			readText(part_id);
			assertChar('\t', in);
			readText(part_name);
			assertChar('\n', in);

			added_parts[part_id] = part_name;
		}

		return added_parts;
	}

	void read_v3(ReadBuffer & in)
	{
		std:string last_added_part;
		readText(last_added_part, in);

		auto partition_info = MergeTreePartInfo::fromPartName(last_added_part, format_version);
        added_parts[partition_info.partition_id] = last_added_part;

        return added_parts;

	}

	void write(WriteBuffer & out, const std::string & new_part_str)
	{
		auto new_part_info = MergeTreePartInfo::fromPartName(new_part_name, format_version);
    
	    added_parts[new_part_info.partition_id] = new_part_name;

	    writeString("parts_count\t", out);
	    writeVarUInt(added_parts.size(), out)
	    writeString("\n");
	    
	    for (const auto & added_part : added_parts)
	    {
	    	writeString(added_part.first);
	    	writeString("\t");
	    	writeVarUInt(added_part.second, out);
	    	writeString("\n");
	    }
	}

	std::string write(const std::string & added_parts_str, const std::string & new_part_str) const
	{
		WriteBufferFromOwnString out;
		read(added_parts_str);
        write(out, str);
        return out.str();
	}
}