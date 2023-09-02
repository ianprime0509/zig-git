const std = @import("std");
const pack = @import("pack.zig");
const Sha1 = std.crypto.hash.Sha1;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var file = try std.fs.cwd().openFile("pack.pack", .{});
    defer file.close();

    var obj_dir = try std.fs.cwd().makeOpenPath("objs", .{});
    defer obj_dir.close();

    var buffered_reader = std.io.bufferedReader(file.reader());
    try pack.unpack(allocator, buffered_reader.reader(), obj_dir);
}
