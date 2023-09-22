const std = @import("std");
const mem = std.mem;
const Sha1 = std.crypto.hash.Sha1;

pub const object_name_length = Sha1.digest_length;
pub const fmt_object_name_length = 2 * object_name_length;
pub const Oid = [object_name_length]u8;

pub fn isOid(s: []const u8) bool {
    return s.len == fmt_object_name_length and for (s) |c| {
        switch (c) {
            '0'...'9', 'a'...'f' => {},
            else => break false,
        }
    } else true;
}

pub fn parseOid(s: []const u8) !Oid {
    if (s.len != fmt_object_name_length) return error.InvalidOid;
    var oid: Oid = undefined;
    for (&oid, 0..) |*b, i| {
        const high = try parseOidDigit(s[2 * i]);
        const low = try parseOidDigit(s[2 * i + 1]);
        b.* = (@as(u8, high) << 4) + low;
    }
    return oid;
}

fn parseOidDigit(d: u8) !u4 {
    return switch (d) {
        '0'...'9' => @intCast(d - '0'),
        'a'...'f' => @intCast(d - 'a' + 10),
        else => error.InvalidOid,
    };
}
