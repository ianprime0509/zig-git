const std = @import("std");
const pack = @import("pack.zig");
const protocol = @import("protocol.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // var file = try std.fs.cwd().openFile("pack.pack", .{});
    // defer file.close();

    // var obj_dir = try std.fs.cwd().makeOpenPath("objs", .{});
    // defer obj_dir.close();

    // var buffered_reader = std.io.bufferedReader(file.reader());
    // try pack.unpack(allocator, buffered_reader.reader(), obj_dir);

    // var client = protocol.Client.init(allocator);
    // defer client.deinit();

    // var output_file = try std.fs.cwd().createFile("download.pack", .{});
    // defer output_file.close();

    // var obj_dir = try std.fs.cwd().makeOpenPath("objs", .{});
    // defer obj_dir.close();

    //const git_uri = std.Uri.parse("http://localhost:8000/cgi-bin/git.cgi/zig-xml") catch unreachable;
    // const git_uri = std.Uri.parse("https://github.com/ianprime0509/zig-xml") catch unreachable;
    // var fetch_stream = try client.fetch(git_uri, &.{"dfdc044f3271641c7d428dc8ec8cd46423d8b8b6"});
    // defer fetch_stream.deinit();
    // var fifo = std.fifo.LinearFifo(u8, .{ .Static = 4096 }).init();
    // try fifo.pump(fetch_stream.reader(), output_file.writer());

    // var buffered_reader = std.io.bufferedReader(fetch_stream.reader());
    // try pack.unpack(allocator, buffered_reader.reader(), obj_dir);

    // try output_file.sync();

    var pack_file = try std.fs.cwd().openFile("download.pack", .{});
    defer pack_file.close();
    var index_file = try std.fs.cwd().createFile("download.idx.new", .{});
    defer index_file.close();
    var index_buffered_writer = std.io.bufferedWriter(index_file.writer());
    try pack.indexPack(allocator, pack_file, index_buffered_writer.writer());
    try index_buffered_writer.flush();
    try index_file.sync();
}
