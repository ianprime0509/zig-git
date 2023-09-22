const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;

pub const max_pkt_line_data = 65516;

pub const Packet = union(enum) {
    flush,
    delimiter,
    response_end,
    data: []const u8,

    /// Reads a single packet in pkt-line format. `buf` must be large enough to
    /// store the packet data (it must be at least `max_pkt_line_data` bytes).
    pub fn read(reader: anytype, buf: []u8) !Packet {
        const length = try parseLength(try reader.readBytesNoEof(4));
        switch (length) {
            0 => return .flush,
            1 => return .delimiter,
            2 => return .response_end,
            3 => return error.InvalidPacket,
            else => if (length - 4 > max_pkt_line_data) return error.InvalidPacket,
        }
        const data = buf[0 .. length - 4];
        try reader.readNoEof(data);
        return .{ .data = data };
    }

    fn parseLength(bytes: [4]u8) !u16 {
        var length: u16 = 0;
        for (bytes, 0..) |b, i| {
            const d: u16 = switch (b) {
                '0'...'9' => b - '0',
                'a'...'f' => b - 'a' + 10,
                else => return error.InvalidPacket,
            };
            length |= d << 4 * (3 - @as(u4, @intCast(i)));
        }
        return length;
    }

    pub fn write(packet: Packet, writer: anytype) !void {
        switch (packet) {
            .flush => try writer.writeAll("0000"),
            .delimiter => try writer.writeAll("0001"),
            .response_end => try writer.writeAll("0002"),
            .data => |data| {
                assert(data.len <= max_pkt_line_data);
                try writer.print("{x:0>4}", .{data.len + 4});
                try writer.writeAll(data);
            },
        }
    }
};

pub const Client = struct {
    // TODO: transport abstraction
    allocator: mem.Allocator,
    transport: std.http.Client,

    pub fn init(allocator: mem.Allocator) Client {
        return .{
            .allocator = allocator,
            .transport = .{ .allocator = allocator },
        };
    }

    pub fn deinit(client: *Client) void {
        client.transport.deinit();
        client.* = undefined;
    }

    pub fn fetch(client: *Client, git_uri: std.Uri, wants: []const []const u8) !FetchStream {
        var fetch_uri = git_uri;
        fetch_uri.path = try std.fs.path.resolvePosix(client.allocator, &.{ "/", git_uri.path, "git-upload-pack" });
        defer client.allocator.free(fetch_uri.path);
        fetch_uri.query = null;
        fetch_uri.fragment = null;

        var headers = std.http.Headers.init(client.allocator);
        defer headers.deinit();
        try headers.append("Content-Type", "application/x-git-upload-pack-request");
        try headers.append("Git-Protocol", "version=2");

        var body = std.ArrayListUnmanaged(u8){};
        defer body.deinit(client.allocator);
        const body_writer = body.writer(client.allocator);
        try Packet.write(.{ .data = "command=fetch\n" }, body_writer);
        try Packet.write(.delimiter, body_writer);
        for (wants) |want| {
            var buf: [max_pkt_line_data]u8 = undefined;
            const arg = std.fmt.bufPrint(&buf, "want {s}\n", .{want}) catch unreachable;
            try Packet.write(.{ .data = arg }, body_writer);
        }
        try Packet.write(.{ .data = "done\n" }, body_writer);
        try Packet.write(.flush, body_writer);

        var request = try client.transport.request(.POST, fetch_uri, headers, .{});
        errdefer request.deinit();
        request.transfer_encoding = .{ .content_length = body.items.len };
        try request.start();
        try request.writeAll(body.items);
        try request.finish();

        try request.wait();

        const reader = request.reader();
        var state: enum { section_start, section_content } = .section_start;
        while (true) {
            var buf: [max_pkt_line_data]u8 = undefined;
            const packet = try Packet.read(reader, &buf);
            switch (state) {
                .section_start => switch (packet) {
                    .data => |data| if (mem.eql(u8, data, "packfile\n")) {
                        return .{ .request = request };
                    } else {
                        state = .section_content;
                    },
                    else => return error.UnexpectedPacket,
                },
                .section_content => switch (packet) {
                    .delimiter => state = .section_start,
                    .data => {},
                    else => return error.UnexpectedPacket,
                },
            }
        }
    }

    pub const FetchStream = struct {
        request: std.http.Client.Request,
        buf: [max_pkt_line_data]u8 = undefined,
        pos: usize = 0,
        len: usize = 0,

        pub fn deinit(stream: *FetchStream) void {
            stream.request.deinit();
        }

        pub const ReadError = std.http.Client.Request.ReadError || error{ InvalidPacket, UnexpectedPacket };
        pub const Reader = std.io.Reader(*FetchStream, ReadError, read);

        pub fn reader(stream: *FetchStream) Reader {
            return .{ .context = stream };
        }

        pub fn read(stream: *FetchStream, buf: []u8) !usize {
            if (stream.pos == stream.len) {
                while (true) {
                    switch (try Packet.read(stream.request.reader(), &stream.buf)) {
                        .flush => return 0,
                        .data => |data| if (data.len > 1 and data[0] == 1) {
                            stream.pos = 1;
                            stream.len = data.len;
                            break;
                        },
                        else => return error.UnexpectedPacket,
                    }
                }
            }

            const size = @min(buf.len, stream.len - stream.pos);
            @memcpy(buf[0..size], stream.buf[stream.pos .. stream.pos + size]);
            stream.pos += size;
            return size;
        }
    };
};
