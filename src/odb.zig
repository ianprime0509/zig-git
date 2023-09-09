const std = @import("std");
const mem = std.mem;
const pack = @import("pack.zig");
const git = @import("git.zig");
const EntryHeader = pack.EntryHeader;
const IndexHeader = pack.IndexHeader;
const Oid = git.Oid;

pub fn Odb(comptime FileType: type) type {
    return struct {
        pack_file: FileType,
        index_header: IndexHeader,
        index_file: FileType,

        const Self = @This();

        pub fn init(pack_file: FileType, index_file: FileType) !Self {
            const index_header = try IndexHeader.read(index_file.reader());
            return .{
                .pack_file = pack_file,
                .index_header = index_header,
                .index_file = index_file,
            };
        }

        pub fn seekEntry(self: *Self, oid: Oid) !EntryHeader {
            const key = oid[0];
            var start_index = if (key > 0) self.index_header.fan_out_table[key - 1] else 0;
            var end_index = self.index_header.fan_out_table[key];
            const found_index = while (start_index < end_index) {
                const mid_index = start_index + (end_index - start_index) / 2;
                try self.index_file.seekTo(IndexHeader.size + mid_index * git.object_name_length);
                const mid_oid = try self.index_file.reader().readBytesNoEof(git.object_name_length);
                switch (mem.order(u8, &mid_oid, &oid)) {
                    .lt => start_index = mid_index + 1,
                    .gt => end_index = mid_index,
                    .eq => break mid_index,
                }
            } else return error.ObjectNotFound;

            const n_objects = self.index_header.fan_out_table[255];
            const offset_values_start = IndexHeader.size + n_objects * (git.object_name_length + 4);
            try self.index_file.seekTo(offset_values_start + found_index * 4);
            const l1_offset: packed struct { value: u31, big: bool } = @bitCast(try self.index_file.reader().readIntBig(u32));
            const pack_offset = pack_offset: {
                if (l1_offset.big) {
                    const l2_offset_values_start = offset_values_start + n_objects * 4;
                    try self.index_file.seekTo(l2_offset_values_start + l1_offset.value * 4);
                    break :pack_offset try self.index_file.reader().readIntBig(u64);
                } else {
                    break :pack_offset l1_offset.value;
                }
            };

            try self.pack_file.seekTo(pack_offset);
            // TODO: buffer?
            return try EntryHeader.read(self.pack_file.reader());
        }
    };
}
