const std = @import("std");
const mem = std.mem;
const git = @import("git.zig");
const Oid = git.Oid;
const Odb = @import("Odb.zig");

odb: Odb,

const Repository = @This();

pub fn checkout(repository: *Repository, allocator: mem.Allocator, worktree: std.fs.Dir, commit_oid: Oid) !void {
    try repository.odb.seekOid(commit_oid);
    const tree_oid = tree_oid: {
        var commit_object = try repository.odb.readObject(allocator);
        defer commit_object.deinit(allocator);
        if (commit_object.type != .commit) return error.NotACommit;
        break :tree_oid try getCommitTree(commit_object.data);
    };
    try repository.checkoutTree(allocator, worktree, tree_oid);
}

fn checkoutTree(repository: *Repository, allocator: mem.Allocator, dir: std.fs.Dir, tree_oid: Oid) !void {
    try repository.odb.seekOid(tree_oid);
    var tree_object = try repository.odb.readObject(allocator);
    defer tree_object.deinit(allocator);
    if (tree_object.type != .tree) return error.NotATree;

    var tree_iter = TreeIterator.init(tree_object.data);
    while (try tree_iter.next()) |entry| {
        switch (entry.type) {
            .directory => {
                try dir.makeDir(entry.name);
                var subdir = try dir.openDir(entry.name, .{});
                defer subdir.close();
                try repository.checkoutTree(allocator, subdir, entry.oid);
            },
            .file => {
                var file = try dir.createFile(entry.name, .{});
                defer file.close();
                try repository.odb.seekOid(entry.oid);
                var file_object = try repository.odb.readObject(allocator);
                defer file_object.deinit(allocator);
                if (file_object.type != .blob) return error.InvalidFile;
                try file.writeAll(file_object.data);
                try file.sync();
            },
            .symlink => return error.SymlinkNotSupported,
            .gitlink => {
                // Consistent with git archive behavior, create the directory but
                // do nothing else
                try dir.makeDir(entry.name);
            },
        }
    }
}

fn getCommitTree(commit_data: []const u8) !Oid {
    if (!mem.startsWith(u8, commit_data, "tree ") or
        commit_data.len < "tree ".len + git.fmt_object_name_length + "\n".len or
        commit_data["tree ".len + git.fmt_object_name_length] != '\n')
    {
        return error.InvalidCommit;
    }
    return try git.parseOid(commit_data["tree ".len..][0..git.fmt_object_name_length]);
}

pub const TreeIterator = struct {
    data: []const u8,
    pos: usize = 0,

    pub fn init(data: []const u8) TreeIterator {
        return .{ .data = data };
    }

    pub const Entry = struct {
        type: Type,
        executable: bool,
        name: [:0]const u8,
        oid: Oid,

        pub const Type = enum(u4) {
            directory = 0o4,
            file = 0o10,
            symlink = 0o12,
            gitlink = 0o16,
        };
    };

    pub fn next(iterator: *TreeIterator) !?Entry {
        if (iterator.pos == iterator.data.len) return null;

        const mode_end = mem.indexOfScalarPos(u8, iterator.data, iterator.pos, ' ') orelse return error.InvalidTree;
        const mode: packed struct {
            permission: u9,
            unused: u3,
            type: u4,
        } = @bitCast(std.fmt.parseUnsigned(u16, iterator.data[iterator.pos..mode_end], 8) catch return error.InvalidTree);
        const @"type" = std.meta.intToEnum(Entry.Type, mode.type) catch return error.InvalidTree;
        const executable = switch (mode.permission) {
            0 => if (@"type" == .file) return error.InvalidTree else false,
            0o644 => if (@"type" != .file) return error.InvalidTree else false,
            0o755 => if (@"type" != .file) return error.InvalidTree else true,
            else => return error.InvalidTree,
        };
        iterator.pos = mode_end + 1;

        const name_end = mem.indexOfScalarPos(u8, iterator.data, iterator.pos, 0) orelse return error.InvalidTree;
        const name = iterator.data[iterator.pos..name_end :0];
        iterator.pos = name_end + 1;

        if (iterator.pos + git.object_name_length > iterator.data.len) return error.InvalidTree;
        const oid = iterator.data[iterator.pos..][0..git.object_name_length].*;
        iterator.pos += git.object_name_length;

        return .{ .type = @"type", .executable = executable, .name = name, .oid = oid };
    }
};
