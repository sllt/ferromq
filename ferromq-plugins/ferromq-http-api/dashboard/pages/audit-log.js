/* ============================================================
   FerroMQ Dashboard — 审计日志（admin）
   ============================================================ */
window.AuditLogPage = Vue.defineComponent({
  name: 'AuditLogPage',
  template: `
    <div>
      <div class="search-bar" style="margin-bottom:16px;">
        <div class="search-row" style="align-items:center;flex-wrap:wrap;gap:8px;">
          <input class="form-input" v-model="action" :placeholder="$t('audit.action')" style="width:180px;" @keyup.enter="load" />
          <input class="form-input" v-model="username" :placeholder="$t('audit.username')" style="width:160px;" @keyup.enter="load" />
          <button class="btn btn-primary" @click="load">{{ $t('audit.search') }}</button>
        </div>
        <p v-if="!canAdmin" style="color:var(--text-muted);margin-top:8px;">{{ $t('audit.admin_only') }}</p>
        <p v-if="error" style="color:var(--danger);margin-top:8px;">{{ error }}</p>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>{{ $t('audit.time') }}</th>
              <th>{{ $t('audit.action') }}</th>
              <th>{{ $t('audit.username') }}</th>
              <th>{{ $t('audit.role') }}</th>
              <th>{{ $t('audit.resource') }}</th>
              <th>{{ $t('audit.success') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="e in events" :key="e.id">
              <td>{{ e.id }}</td>
              <td>{{ formatTs(e.ts) }}</td>
              <td>{{ e.action }}</td>
              <td>{{ e.username }}</td>
              <td>{{ e.role }} / {{ e.auth }}</td>
              <td>{{ e.resource || '-' }}</td>
              <td>{{ e.success ? $t('audit.ok') : $t('audit.fail') }}</td>
            </tr>
            <tr v-if="events.length === 0">
              <td colspan="7" style="text-align:center;color:var(--text-muted);padding:40px;">{{ $t('audit.empty') }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  `,
  setup() {
    function $t(key, params) { return window.i18n.$t(key, params); }
    const events = Vue.ref([]);
    const action = Vue.ref('');
    const username = Vue.ref('');
    const error = Vue.ref('');
    const canAdmin = Vue.computed(function() { return window.store.isAdmin(); });

    function formatTs(ts) {
      if (!ts) return '-';
      try { return new Date(ts).toISOString(); } catch (_) { return String(ts); }
    }
    async function load() {
      error.value = '';
      try {
        const data = await http.get('/audit', {
          action: action.value,
          username: username.value,
          _limit: 200,
        });
        events.value = Array.isArray(data) ? data : (data && data.items) || [];
      } catch (e) { error.value = e.message; }
    }
    load();
    return { $t, events, action, username, error, canAdmin, load, formatTs };
  },
});
