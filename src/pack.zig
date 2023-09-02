// TODO: clean up errors
const std = @import("std");
const mem = std.mem;
const Sha1 = std.crypto.hash.Sha1;

pub const object_name_length = Sha1.digest_length;

pub const Entry = union(Type) {
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
        base_object: [object_name_length]u8,
        uncompressed_length: u64,
    };
};

pub fn unpack(allocator: mem.Allocator, packfile_reader: anytype, dir: std.fs.Dir) !void {
    var counting_reader = std.io.countingReader(packfile_reader);
    var pack_reader = try reader(counting_reader.reader());

    var objects_read: u32 = 0;
    var objects_by_index = std.AutoHashMapUnmanaged(u64, [object_name_length]u8){};
    defer objects_by_index.deinit(allocator);
    while (objects_read < pack_reader.total_objects) : (objects_read += 1) {
        const entry_start = counting_reader.bytes_read;
        const entry = try pack_reader.next();

        switch (entry) {
            inline .commit, .tree, .blob, .tag => |obj, tag| {
                var entry_stream = try pack_reader.streamEntry(allocator);
                defer entry_stream.deinit();
                var entry_counting_reader = std.io.countingReader(entry_stream.reader());

                const hash = hash: {
                    var tmp_file = try dir.createFile("tmp", .{});
                    defer tmp_file.close();
                    var tmp_buffered_writer = std.io.bufferedWriter(tmp_file.writer());
                    var tmp_hashed_writer = hashedWriter(tmp_buffered_writer.writer(), Sha1.init(.{}));
                    try tmp_hashed_writer.writer().print("{s} {}\x00", .{ @tagName(tag), obj.uncompressed_length });
                    var fifo = std.fifo.LinearFifo(u8, .{ .Static = 4096 }).init();
                    try fifo.pump(entry_counting_reader.reader(), tmp_hashed_writer.writer());
                    try tmp_buffered_writer.flush();
                    try tmp_file.sync();
                    break :hash tmp_hashed_writer.hasher.finalResult();
                };

                if (entry_counting_reader.bytes_read != obj.uncompressed_length) {
                    return error.InvalidFormat;
                }
                var hash_buf: [object_name_length * 2]u8 = undefined;
                const hash_fmt = std.fmt.bufPrint(&hash_buf, "{}", .{std.fmt.fmtSliceHexLower(&hash)}) catch unreachable;
                try dir.rename("tmp", hash_fmt);
                try objects_by_index.put(allocator, entry_start, hash);
            },
            .ofs_delta => |delta| {
                var entry_stream = try pack_reader.streamEntry(allocator);
                defer entry_stream.deinit();
                var entry_counting_reader = std.io.countingReader(entry_stream.reader());

                if (delta.offset > entry_start) return error.InvalidFormat;
                const base_hash = objects_by_index.get(entry_start - delta.offset) orelse return error.InvalidFormat;
                var base_hash_buf: [object_name_length * 2]u8 = undefined;
                const base_hash_fmt = std.fmt.bufPrint(&base_hash_buf, "{}", .{std.fmt.fmtSliceHexLower(&base_hash)}) catch unreachable;
                var base_file = try dir.openFile(base_hash_fmt, .{});
                defer base_file.close();

                const hash = hash: {
                    var tmp_file = try dir.createFile("tmp", .{});
                    defer tmp_file.close();
                    var tmp_buffered_writer = std.io.bufferedWriter(tmp_file.writer());
                    var tmp_hashed_writer = hashedWriter(tmp_buffered_writer.writer(), Sha1.init(.{}));
                    try expandDelta(base_file, entry_counting_reader.reader(), tmp_hashed_writer.writer());
                    try tmp_buffered_writer.flush();
                    try tmp_file.sync();
                    break :hash tmp_hashed_writer.hasher.finalResult();
                };

                if (entry_counting_reader.bytes_read != delta.uncompressed_length) {
                    return error.InvalidFormat;
                }
                var hash_buf: [object_name_length * 2]u8 = undefined;
                const hash_fmt = std.fmt.bufPrint(&hash_buf, "{}", .{std.fmt.fmtSliceHexLower(&hash)}) catch unreachable;
                try dir.rename("tmp", hash_fmt);
                try objects_by_index.put(allocator, entry_start, hash);
            },
            else => {
                // TODO: handle
                var entry_stream = try pack_reader.streamEntry(allocator);
                defer entry_stream.deinit();
                var buf: [4096]u8 = undefined;
                while (true) {
                    const read = try entry_stream.reader().readAll(&buf);
                    if (read < buf.len) break;
                }
            },
        }
    }
    try pack_reader.finish();
}

pub fn reader(r: anytype) (@TypeOf(r).Error || error{ InvalidHeader, UnsupportedVersion })!Reader(@TypeOf(r)) {
    var hashed_reader = std.compress.hashedReader(r, Sha1.init(.{}));
    const input = hashed_reader.reader();

    const signature = input.readBytesNoEof(4) catch |e| switch (e) {
        error.EndOfStream => return error.InvalidHeader,
        else => |other| return other,
    };
    if (!mem.eql(u8, &signature, "PACK")) return error.InvalidHeader;
    const version = input.readIntBig(u32) catch |e| switch (e) {
        error.EndOfStream => return error.InvalidHeader,
        else => |other| return other,
    };
    if (version != 2) return error.UnsupportedVersion;
    const total_objects = input.readIntBig(u32) catch |e| switch (e) {
        error.EndOfStream => return error.InvalidHeader,
        else => |other| return other,
    };

    return .{
        .hashed_reader = hashed_reader,
        .total_objects = total_objects,
    };
}

pub fn Reader(comptime ReaderType: type) type {
    return struct {
        hashed_reader: HashedReader,
        total_objects: u32,

        const Self = @This();
        const HashedReader = std.compress.HashedReader(ReaderType, Sha1);

        pub fn next(self: *Self) !Entry {
            const r = self.hashed_reader.reader();
            const InitialByte = packed struct { len: u4, type: u3, has_next: bool };
            const initial: InitialByte = @bitCast(r.readByte() catch |e| switch (e) {
                error.EndOfStream => return error.InvalidFormat,
                else => |other| return other,
            });
            const rest_len = if (initial.has_next) try readSizeVarInt(r) else 0;
            var uncompressed_length: u64 = initial.len;
            uncompressed_length |= std.math.shlExact(u64, rest_len, 4) catch return error.InvalidFormat;
            const @"type" = std.meta.intToEnum(Entry.Type, initial.type) catch return error.InvalidFormat;
            return switch (@"type") {
                inline .commit, .tree, .blob, .tag => |tag| @unionInit(Entry, @tagName(tag), .{
                    .uncompressed_length = uncompressed_length,
                }),
                .ofs_delta => .{ .ofs_delta = .{
                    .offset = try readOffsetVarInt(r),
                    .uncompressed_length = uncompressed_length,
                } },
                .ref_delta => .{ .ref_delta = .{
                    .base_object = r.readBytesNoEof(object_name_length) catch |e| switch (e) {
                        error.EndOfStream => return error.InvalidFormat,
                        else => |other| return other,
                    },
                    .uncompressed_length = uncompressed_length,
                } },
            };
        }

        pub fn streamEntry(self: *Self, allocator: mem.Allocator) !std.compress.zlib.DecompressStream(HashedReader.Reader) {
            return std.compress.zlib.decompressStream(allocator, self.hashed_reader.reader());
        }

        pub fn finish(self: *Self) !void {
            const r = self.hashed_reader.child_reader;
            const expected_hash = r.readBytesNoEof(Sha1.digest_length) catch |e| switch (e) {
                error.EndOfStream => return error.InvalidFormat,
                else => |other| return other,
            };
            const actual_hash = self.hashed_reader.hasher.finalResult();
            if (!mem.eql(u8, &expected_hash, &actual_hash)) {
                return error.InvalidFormat;
            }
            // TODO: better way to assert EOF?
            _ = self.hashed_reader.child_reader.readByte() catch |e| switch (e) {
                error.EndOfStream => return,
                else => |other| return other,
            };
            return error.InvalidFormat;
        }
    };
}

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
            return error.InvalidFormat;
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
