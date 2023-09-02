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

    pub fn write(self: Packet, writer: anytype) !void {
        switch (self) {
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

    pub fn deinit(self: *Client) void {
        self.transport.deinit();
        self.* = undefined;
    }

    pub fn fetch(self: *Client, git_uri: std.Uri, wants: []const []const u8) !FetchStream {
        var fetch_uri = git_uri;
        fetch_uri.path = try std.fs.path.resolvePosix(self.allocator, &.{ "/", git_uri.path, "git-upload-pack" });
        defer self.allocator.free(fetch_uri.path);
        fetch_uri.query = null;
        fetch_uri.fragment = null;

        var headers = std.http.Headers.init(self.allocator);
        defer headers.deinit();
        try headers.append("Content-Type", "application/x-git-upload-pack-request");
        try headers.append("Git-Protocol", "version=2");

        var body = std.ArrayListUnmanaged(u8){};
        defer body.deinit(self.allocator);
        const body_writer = body.writer(self.allocator);
        try Packet.write(.{ .data = "command=fetch\n" }, body_writer);
        try Packet.write(.delimiter, body_writer);
        for (wants) |want| {
            var buf: [max_pkt_line_data]u8 = undefined;
            const arg = std.fmt.bufPrint(&buf, "want {s}\n", .{want}) catch unreachable;
            try Packet.write(.{ .data = arg }, body_writer);
        }
        try Packet.write(.{ .data = "done\n" }, body_writer);
        try Packet.write(.flush, body_writer);

        var request = try self.transport.request(.POST, fetch_uri, headers, .{});
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

        pub fn deinit(self: *FetchStream) void {
            self.request.deinit();
        }

        pub const ReadError = std.http.Client.Request.ReadError || error{ InvalidPacket, UnexpectedPacket };
        pub const Reader = std.io.Reader(*FetchStream, ReadError, read);

        pub fn reader(self: *FetchStream) Reader {
            return .{ .context = self };
        }

        pub fn read(self: *FetchStream, buf: []u8) !usize {
            if (self.pos == self.len) {
                while (true) {
                    switch (try Packet.read(self.request.reader(), &self.buf)) {
                        .flush => return 0,
                        .data => |data| if (data.len > 1 and data[0] == 1) {
                            self.pos = 1;
                            self.len = data.len;
                            break;
                        },
                        else => return error.UnexpectedPacket,
                    }
                }
            }

            const size = @min(buf.len, self.len - self.pos);
            @memcpy(buf[0..size], self.buf[self.pos .. self.pos + size]);
            self.pos += size;
            return size;
        }
    };
};
