// TODO: clean up errors
const std = @import("std");
const mem = std.mem;
const Sha1 = std.crypto.hash.Sha1;
const git = @import("git.zig");
const Oid = git.Oid;

pub const PackHeader = struct {
    total_objects: u32,

    pub const signature = "PACK";
    pub const supported_version = 2;

    pub fn read(reader: anytype) (@TypeOf(reader).Error || error{ InvalidHeader, UnsupportedVersion })!PackHeader {
        const actual_signature = reader.readBytesNoEof(4) catch |e| switch (e) {
            error.EndOfStream => return error.InvalidHeader,
            else => |other| return other,
        };
        if (!mem.eql(u8, &actual_signature, signature)) return error.InvalidHeader;
        const version = reader.readIntBig(u32) catch |e| switch (e) {
            error.EndOfStream => return error.InvalidHeader,
            else => |other| return other,
        };
        if (version != supported_version) return error.UnsupportedVersion;
        const total_objects = reader.readIntBig(u32) catch |e| switch (e) {
            error.EndOfStream => return error.InvalidHeader,
            else => |other| return other,
        };
        return .{ .total_objects = total_objects };
    }
};

pub const EntryHeader = union(Type) {
    commit: Undeltified,
    tree: Undeltified,
    blob: Undeltified,
    tag: Undeltified,
    ofs_delta: OfsDelta,
    ref_delta: RefDelta,

    pub const Type = enum(u3) {
        commit = 1,
        tree = 2,
        blob = 3,
        tag = 4,
        ofs_delta = 6,
        ref_delta = 7,
    };

    pub const Undeltified = struct {
        uncompressed_length: u64,
    };

    pub const OfsDelta = struct {
        offset: u64,
        uncompressed_length: u64,
    };

    pub const RefDelta = struct {
        base_object: Oid,
        uncompressed_length: u64,
    };

    pub fn uncompressedLength(self: EntryHeader) u64 {
        return switch (self) {
            inline else => |entry| entry.uncompressed_length,
        };
    }

    pub fn read(reader: anytype) (@TypeOf(reader).Error || error{ EndOfStream, InvalidFormat })!EntryHeader {
        const InitialByte = packed struct { len: u4, type: u3, has_next: bool };
        const initial: InitialByte = @bitCast(reader.readByte() catch |e| switch (e) {
            error.EndOfStream => return error.InvalidFormat,
            else => |other| return other,
        });
        const rest_len = if (initial.has_next) try readSizeVarInt(reader) else 0;
        var uncompressed_length: u64 = initial.len;
        uncompressed_length |= std.math.shlExact(u64, rest_len, 4) catch return error.InvalidFormat;
        const @"type" = std.meta.intToEnum(EntryHeader.Type, initial.type) catch return error.InvalidFormat;
        return switch (@"type") {
            inline .commit, .tree, .blob, .tag => |tag| @unionInit(EntryHeader, @tagName(tag), .{
                .uncompressed_length = uncompressed_length,
            }),
            .ofs_delta => .{ .ofs_delta = .{
                .offset = try readOffsetVarInt(reader),
                .uncompressed_length = uncompressed_length,
            } },
            .ref_delta => .{ .ref_delta = .{
                .base_object = reader.readBytesNoEof(git.object_name_length) catch |e| switch (e) {
                    error.EndOfStream => return error.InvalidFormat,
                    else => |other| return other,
                },
                .uncompressed_length = uncompressed_length,
            } },
        };
    }
};

fn readSizeVarInt(r: anytype) !u64 {
    const Byte = packed struct { value: u7, has_next: bool };
    var b: Byte = @bitCast(try r.readByte());
    var value: u64 = b.value;
    var shift: u6 = 0;
    while (b.has_next) {
        b = @bitCast(try r.readByte());
        shift = std.math.add(u6, shift, 7) catch return error.InvalidFormat;
        value |= @as(u64, b.value) << shift;
    }
    return value;
}

fn readOffsetVarInt(r: anytype) !u64 {
    const Byte = packed struct { value: u7, has_next: bool };
    var b: Byte = @bitCast(try r.readByte());
    var value: u64 = b.value;
    while (b.has_next) {
        b = @bitCast(try r.readByte());
        value = std.math.shlExact(u64, value + 1, 7) catch return error.InvalidFormat;
        value |= b.value;
    }
    return value;
}

pub const IndexHeader = struct {
    fan_out_table: [256]u32,

    pub const signature = "\xFFtOc";
    pub const supported_version = 2;
    pub const size = 4 + 4 + @sizeOf([256]u32);

    pub fn read(reader: anytype) !IndexHeader {
        var header_bytes = try reader.readBytesNoEof(size);
        if (!mem.eql(u8, header_bytes[0..4], signature)) return error.InvalidHeader;
        const version = mem.readIntBig(u32, header_bytes[4..8]);
        if (version != supported_version) return error.UnsupportedVersion;
        const fan_out_table_slice: *[256]u32 = @alignCast(@ptrCast(header_bytes[8..]));
        if (@import("builtin").cpu.arch.endian() != .Big) {
            for (fan_out_table_slice) |*entry| {
                entry.* = @byteSwap(entry.*);
            }
        }
        return .{ .fan_out_table = fan_out_table_slice.* };
    }
};

const IndexEntry = struct {
    offset: u64,
    crc32: u32,
};

pub fn indexPack(allocator: mem.Allocator, pack: anytype, index_writer: anytype) !void {
    // TODO: this would be better if we had a TreeMap
    var index_entries = std.AutoHashMapUnmanaged(Oid, IndexEntry){};
    defer index_entries.deinit(allocator);
    var pending_deltas = std.ArrayListUnmanaged(IndexEntry){};
    defer pending_deltas.deinit(allocator);

    const pack_checksum = try indexPackFirstPass(allocator, pack, &index_entries, &pending_deltas);
    // TODO: add reconstructed object cache
    var remaining_deltas = pending_deltas.items.len;
    while (remaining_deltas > 0) {
        var i: usize = remaining_deltas;
        while (i > 0) {
            i -= 1;
            const delta = pending_deltas.items[i];
            if (try indexPackHashDelta(allocator, pack, delta, index_entries)) |oid| {
                try index_entries.put(allocator, oid, delta);
                _ = pending_deltas.swapRemove(i);
            }
        }
        if (pending_deltas.items.len == remaining_deltas) return error.MissingRefs;
        remaining_deltas = pending_deltas.items.len;
    }

    var oids = std.ArrayListUnmanaged(Oid){};
    defer oids.deinit(allocator);
    try oids.ensureTotalCapacityPrecise(allocator, index_entries.count());
    var index_entries_iter = index_entries.iterator();
    while (index_entries_iter.next()) |entry| {
        oids.appendAssumeCapacity(entry.key_ptr.*);
    }
    mem.sortUnstable(Oid, oids.items, {}, struct {
        fn lessThan(_: void, o1: Oid, o2: Oid) bool {
            return mem.lessThan(u8, &o1, &o2);
        }
    }.lessThan);

    var fan_out_table: [256]u32 = undefined;
    var count: u32 = 0;
    var fan_out_index: u8 = 0;
    for (oids.items) |oid| {
        if (oid[0] > fan_out_index) {
            @memset(fan_out_table[fan_out_index..oid[0]], count);
            fan_out_index = oid[0];
        }
        count += 1;
    }
    @memset(fan_out_table[fan_out_index..], count);

    var index_hashed_writer = hashedWriter(index_writer, Sha1.init(.{}));
    const writer = index_hashed_writer.writer();
    try writer.writeAll(IndexHeader.signature);
    try writer.writeIntBig(u32, IndexHeader.supported_version);
    for (fan_out_table) |fan_out_entry| {
        try writer.writeIntBig(u32, fan_out_entry);
    }

    for (oids.items) |oid| {
        try writer.writeAll(&oid);
    }

    for (oids.items) |oid| {
        try writer.writeIntBig(u32, index_entries.get(oid).?.crc32);
    }

    var big_offsets = std.ArrayListUnmanaged(u64){};
    defer big_offsets.deinit(allocator);
    for (oids.items) |oid| {
        const offset = index_entries.get(oid).?.offset;
        if (offset <= std.math.maxInt(u31)) {
            try writer.writeIntBig(u32, @intCast(offset));
        } else {
            const index = big_offsets.items.len;
            try big_offsets.append(allocator, offset);
            try writer.writeIntBig(u32, @as(u32, @intCast(index)) | (1 << 31));
        }
    }
    for (big_offsets.items) |offset| {
        try writer.writeIntBig(u64, offset);
    }

    try writer.writeAll(&pack_checksum);
    const index_checksum = index_hashed_writer.hasher.finalResult();
    try index_writer.writeAll(&index_checksum);
}

fn indexPackFirstPass(
    allocator: mem.Allocator,
    pack: anytype,
    index_entries: *std.AutoHashMapUnmanaged(Oid, IndexEntry),
    pending_deltas: *std.ArrayListUnmanaged(IndexEntry),
) ![Sha1.digest_length]u8 {
    var pack_buffered_reader = std.io.bufferedReader(pack.reader());
    var pack_counting_reader = std.io.countingReader(pack_buffered_reader.reader());
    var pack_hashed_reader = std.compress.hashedReader(pack_counting_reader.reader(), Sha1.init(.{}));
    const pack_reader = pack_hashed_reader.reader();

    const pack_header = try PackHeader.read(pack_reader);

    var current_entry: u32 = 0;
    while (current_entry < pack_header.total_objects) : (current_entry += 1) {
        const entry_offset = pack_counting_reader.bytes_read;
        var entry_crc32_reader = std.compress.hashedReader(pack_reader, std.hash.Crc32.init());
        const entry_header = try EntryHeader.read(entry_crc32_reader.reader());
        switch (entry_header) {
            inline .commit, .tree, .blob, .tag => |object, tag| {
                var entry_decompress_stream = try std.compress.zlib.decompressStream(allocator, entry_crc32_reader.reader());
                defer entry_decompress_stream.deinit();
                var entry_counting_reader = std.io.countingReader(entry_decompress_stream.reader());
                var entry_hashed_writer = hashedWriter(std.io.null_writer, Sha1.init(.{}));
                const entry_writer = entry_hashed_writer.writer();
                // The object header is not included in the pack data but is
                // part of the object's ID
                try entry_writer.print("{s} {}\x00", .{ @tagName(tag), object.uncompressed_length });
                var fifo = std.fifo.LinearFifo(u8, .{ .Static = 4096 }).init();
                try fifo.pump(entry_counting_reader.reader(), entry_writer);
                if (entry_counting_reader.bytes_read != object.uncompressed_length) {
                    return error.InvalidObject;
                }
                const oid = entry_hashed_writer.hasher.finalResult();
                try index_entries.put(allocator, oid, .{
                    .offset = entry_offset,
                    .crc32 = entry_crc32_reader.hasher.final(),
                });
            },
            inline .ofs_delta, .ref_delta => |delta| {
                var entry_decompress_stream = try std.compress.zlib.decompressStream(allocator, entry_crc32_reader.reader());
                defer entry_decompress_stream.deinit();
                var entry_counting_reader = std.io.countingReader(entry_decompress_stream.reader());
                var fifo = std.fifo.LinearFifo(u8, .{ .Static = 4096 }).init();
                try fifo.pump(entry_counting_reader.reader(), std.io.null_writer);
                if (entry_counting_reader.bytes_read != delta.uncompressed_length) {
                    return error.InvalidObject;
                }
                try pending_deltas.append(allocator, .{
                    .offset = entry_offset,
                    .crc32 = entry_crc32_reader.hasher.final(),
                });
            },
        }
    }

    const pack_checksum = pack_hashed_reader.hasher.finalResult();
    const recorded_checksum = try pack_buffered_reader.reader().readBytesNoEof(Sha1.digest_length);
    if (!mem.eql(u8, &pack_checksum, &recorded_checksum)) {
        return error.CorruptedPack;
    }
    // TODO: better way to ensure EOF?
    _ = pack_buffered_reader.reader().readByte() catch |e| switch (e) {
        error.EndOfStream => return pack_checksum,
        else => |other| return other,
    };
    return error.InvalidFormat;
}

fn indexPackHashDelta(
    allocator: mem.Allocator,
    pack: anytype,
    delta: IndexEntry,
    index_entries: std.AutoHashMapUnmanaged(Oid, IndexEntry),
) !?Oid {
    var pending_entries = std.ArrayListUnmanaged(u64){};
    defer pending_entries.deinit(allocator);

    var current_offset = delta.offset;
    while (true) {
        try pack.seekTo(current_offset);
        var entry_buffered_reader = std.io.bufferedReader(pack.reader());
        const entry_header = try EntryHeader.read(entry_buffered_reader.reader());
        try pending_entries.append(allocator, current_offset);
        switch (entry_header) {
            .commit, .tree, .blob, .tag => break,
            .ofs_delta => |ofs_delta| current_offset = std.math.sub(u64, current_offset, ofs_delta.offset) catch return error.InvalidObject,
            .ref_delta => |ref_delta| current_offset = (index_entries.get(ref_delta.base_object) orelse return null).offset,
        }
    }

    var previous_entry = std.ArrayListUnmanaged(u8){};
    defer previous_entry.deinit(allocator);
    var base_entry_header: EntryHeader = undefined;
    {
        const entry_offset = pending_entries.pop();
        try pack.seekTo(entry_offset);
        var entry_buffered_reader = std.io.bufferedReader(pack.reader());
        base_entry_header = try EntryHeader.read(entry_buffered_reader.reader());
        var entry_decompress_stream = try std.compress.zlib.decompressStream(allocator, entry_buffered_reader.reader());
        defer entry_decompress_stream.deinit();
        const entry_writer = previous_entry.writer(allocator);
        var fifo = std.fifo.LinearFifo(u8, .{ .Static = 4096 }).init();
        try fifo.pump(entry_decompress_stream.reader(), entry_writer);
    }

    var current_entry = std.ArrayListUnmanaged(u8){};
    defer current_entry.deinit(allocator);
    while (pending_entries.popOrNull()) |entry_offset| {
        current_entry.clearRetainingCapacity();

        try pack.seekTo(entry_offset);
        var entry_buffered_reader = std.io.bufferedReader(pack.reader());
        _ = try EntryHeader.read(entry_buffered_reader.reader());
        var entry_decompress_stream = try std.compress.zlib.decompressStream(allocator, entry_buffered_reader.reader());
        defer entry_decompress_stream.deinit();
        var base_object = std.io.fixedBufferStream(previous_entry.items);
        try expandDelta(&base_object, entry_decompress_stream.reader(), current_entry.writer(allocator));

        mem.swap(std.ArrayListUnmanaged(u8), &previous_entry, &current_entry);
    }

    var entry_hasher = Sha1.init(.{});
    var entry_hashed_writer = hashedWriter(std.io.null_writer, &entry_hasher);
    try entry_hashed_writer.writer().print("{s} {}\x00", .{ @tagName(base_entry_header), previous_entry.items.len });
    entry_hasher.update(previous_entry.items);
    return entry_hasher.finalResult();
}

pub fn expandDelta(base_object: anytype, delta_reader: anytype, writer: anytype) !void {
    _ = try readSizeVarInt(delta_reader); // base object size
    _ = try readSizeVarInt(delta_reader); // expanded object size
    while (true) {
        const inst: packed struct { value: u7, copy: bool } = @bitCast(delta_reader.readByte() catch |e| switch (e) {
            error.EndOfStream => return,
            else => |other| return other,
        });
        if (inst.copy) {
            const available: packed struct {
                offset1: bool,
                offset2: bool,
                offset3: bool,
                offset4: bool,
                size1: bool,
                size2: bool,
                size3: bool,
            } = @bitCast(inst.value);
            var offset_parts: packed struct { offset1: u8, offset2: u8, offset3: u8, offset4: u8 } = undefined;
            offset_parts.offset1 = if (available.offset1) try delta_reader.readByte() else 0;
            offset_parts.offset2 = if (available.offset2) try delta_reader.readByte() else 0;
            offset_parts.offset3 = if (available.offset3) try delta_reader.readByte() else 0;
            offset_parts.offset4 = if (available.offset4) try delta_reader.readByte() else 0;
            const offset: u32 = @bitCast(offset_parts);
            var size_parts: packed struct { size1: u8, size2: u8, size3: u8 } = undefined;
            size_parts.size1 = if (available.size1) try delta_reader.readByte() else 0;
            size_parts.size2 = if (available.size2) try delta_reader.readByte() else 0;
            size_parts.size3 = if (available.size3) try delta_reader.readByte() else 0;
            var size: u24 = @bitCast(size_parts);
            if (size == 0) size = 0x10000;
            try base_object.seekTo(offset);
            var copy_reader = std.io.limitedReader(base_object.reader(), size);
            var fifo = std.fifo.LinearFifo(u8, .{ .Static = 4096 }).init();
            try fifo.pump(copy_reader.reader(), writer);
        } else if (inst.value != 0) {
            var data_reader = std.io.limitedReader(delta_reader, inst.value);
            var fifo = std.fifo.LinearFifo(u8, .{ .Static = 4096 }).init();
            try fifo.pump(data_reader.reader(), writer);
        } else {
            return error.InvalidDeltaInstruction;
        }
    }
}

fn HashedWriter(
    comptime WriterType: anytype,
    comptime HasherType: anytype,
) type {
    return struct {
        child_writer: WriterType,
        hasher: HasherType,

        pub const Error = WriterType.Error;
        pub const Writer = std.io.Writer(*@This(), Error, write);

        pub fn write(self: *@This(), buf: []const u8) Error!usize {
            const amt = try self.child_writer.write(buf);
            self.hasher.update(buf);
            return amt;
        }

        pub fn writer(self: *@This()) Writer {
            return .{ .context = self };
        }
    };
}

fn hashedWriter(
    writer: anytype,
    hasher: anytype,
) HashedWriter(@TypeOf(writer), @TypeOf(hasher)) {
    return .{ .child_writer = writer, .hasher = hasher };
}
