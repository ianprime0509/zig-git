const std = @import("std");
const mem = std.mem;
const pack = @import("pack.zig");
const git = @import("git.zig");
const EntryHeader = pack.EntryHeader;
const IndexHeader = pack.IndexHeader;
const Oid = git.Oid;

pub const Object = struct {
    type: Type,
    data: []const u8,

    pub const Type = enum {
        commit,
        tree,
        blob,
        tag,
    };

    pub fn deinit(object: *Object, allocator: mem.Allocator) void {
        allocator.free(object.data);
        object.* = undefined;
    }
};

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

        pub fn readObject(self: *Self, allocator: mem.Allocator) !Object {
            // Figure out the chain of deltas to resolve
            // TODO: delta base cache
            var base_offset = try self.pack_file.getPos();
            // TODO: buffer header read
            var base_header = try EntryHeader.read(self.pack_file.reader());
            var delta_offsets = std.ArrayListUnmanaged(u64){};
            defer delta_offsets.deinit(allocator);
            while (true) {
                switch (base_header) {
                    .ofs_delta => |ofs_delta| {
                        try delta_offsets.append(allocator, base_offset);
                        // TODO: check for overflow
                        base_offset -= ofs_delta.offset;
                        try self.pack_file.seekTo(base_offset);
                        base_header = try EntryHeader.read(self.pack_file.reader());
                    },
                    .ref_delta => |ref_delta| {
                        try delta_offsets.append(allocator, base_offset);
                        try self.seekOid(ref_delta.base_object);
                        base_offset = try self.pack_file.getPos();
                        base_header = try EntryHeader.read(self.pack_file.reader());
                    },
                    else => break,
                }
            }

            // Resolve deltas (if any). We start at the base object.
            var base_data = try self.readObjectRaw(allocator, base_header.uncompressedLength());
            errdefer allocator.free(base_data);
            while (delta_offsets.popOrNull()) |delta_offset| {
                try self.pack_file.seekTo(delta_offset);
                const delta_header = try EntryHeader.read(self.pack_file.reader());
                var delta_data = try self.readObjectRaw(allocator, delta_header.uncompressedLength());
                errdefer allocator.free(delta_data);
                var delta_stream = std.io.fixedBufferStream(delta_data);
                const delta_reader = delta_stream.reader();
                _ = try pack.readSizeVarInt(delta_reader); // base object size
                const expanded_size = try pack.readSizeVarInt(delta_reader);
                // TODO: audit/check all @intCasts and such
                var expanded_data = try allocator.alloc(u8, @intCast(expanded_size));
                errdefer allocator.free(expanded_data);
                var expanded_delta_stream = std.io.fixedBufferStream(expanded_data);
                var base_stream = std.io.fixedBufferStream(base_data);
                try pack.expandDelta(&base_stream, delta_reader, expanded_delta_stream.writer());
                if (expanded_delta_stream.pos != expanded_size) return error.InvalidObject;
                allocator.free(base_data);
                allocator.free(delta_data);
                base_data = expanded_data;
            }

            return .{
                .type = switch (base_header) {
                    inline .commit, .tree, .blob, .tag => |_, tag| @field(Object.Type, @tagName(tag)),
                    else => unreachable,
                },
                .data = base_data,
            };
        }

        pub fn readObjectRaw(self: *Self, allocator: mem.Allocator, size: usize) ![]u8 {
            var buffered_reader = std.io.bufferedReader(self.pack_file.reader());
            var decompress_stream = try std.compress.zlib.decompressStream(allocator, buffered_reader.reader());
            defer decompress_stream.deinit();
            var data = try allocator.alloc(u8, size);
            errdefer allocator.free(data);
            try decompress_stream.reader().readNoEof(data);
            // TODO: assert EOF
            return data;
        }

        pub fn seekOid(self: *Self, oid: Oid) !void {
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
        }
    };
}
