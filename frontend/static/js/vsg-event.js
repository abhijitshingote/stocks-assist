/* Unified VSG last-event formatter.
   last_event_type: 'volume_spike' | 'gapper' | 'both'
   last_event_magnitude: volume ratio (always)
   last_event_return: close/prev−1 (always)
*/
(function (root) {
    function isSpike(s) {
        const t = s && s.last_event_type;
        return t === 'volume_spike' || t === 'both';
    }
    function isGap(s) {
        const t = s && s.last_event_type;
        return t === 'gapper' || t === 'both';
    }
    function badge(s) {
        const sp = isSpike(s), g = isGap(s);
        if (sp && g) return 'SG';
        if (sp) return 'S';
        if (g) return 'G';
        return '';
    }
    function badgeClass(s) {
        const sp = isSpike(s), g = isGap(s);
        if (sp && g) return 'both';
        if (sp) return 'spike';
        if (g) return 'gap';
        return '';
    }
    function tagClass(s) {
        const c = badgeClass(s);
        if (c === 'gap') return 'gapper';
        return c || 'event';
    }
    function fmtPct(ret, digits) {
        if (ret == null) return '';
        const d = digits == null ? 1 : digits;
        return (ret >= 0 ? '+' : '') + (ret * 100).toFixed(d) + '%';
    }
    function magStr(s, opts) {
        opts = opts || {};
        const compact = !!opts.compact;
        const digits = opts.digits != null ? opts.digits : (compact ? 0 : 1);
        const pct = fmtPct(s && s.last_event_return, digits);
        const vol = s && s.last_event_magnitude != null
            ? s.last_event_magnitude.toFixed(1) + 'x' : '';
        const pctPart = pct ? pct + (isGap(s) ? ' gap' : '') : '';
        if (pct && vol) {
            return compact ? pct + ' ' + vol : pctPart + ' on ' + vol + ' vol';
        }
        if (pctPart) return pctPart;
        if (vol) return compact ? vol : vol + ' vol';
        return '';
    }
    function extraHtml(s) {
        const b = badge(s);
        const c = badgeClass(s);
        const mag = magStr(s);
        return (b ? '<span class="mini-badge ' + c + '">' + b + '</span>' : '') +
            (mag ? '<span class="list-extra event-mag">' + mag + '</span>' : '');
    }

    root.VsgEvent = {
        isSpike: isSpike,
        isGap: isGap,
        badge: badge,
        badgeClass: badgeClass,
        tagClass: tagClass,
        magStr: magStr,
        extraHtml: extraHtml,
    };
})(typeof window !== 'undefined' ? window : this);
