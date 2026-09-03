/* ============================================================
   FerroMQ Dashboard — API Keys（admin）
   ============================================================ */
window.ApiKeysPage = Vue.defineComponent({
  name: 'ApiKeysPage',
  template: `
    <div>
      <div class="search-bar" style="margin-bottom:16px;">
        <div class="search-row" style="align-items:flex-end;flex-wrap:wrap;gap:8px;">
          <div class="form-group" style="margin:0;">
            <label>{{ $t('api_keys.name') }}</label>
            <input class="form-input" v-model="newName" />
          </div>
          <div class="form-group" style="margin:0;">
            <label>{{ $t('api_keys.role') }}</label>
            <select class="form-select" v-model="newRole" style="width:140px;">
              <option value="admin">admin</option>
              <option value="operator">operator</option>
              <option value="viewer">viewer</option>
            </select>
          </div>
          <button class="btn btn-primary" @click="createKey" :disabled="!canAdmin || creating">
            {{ $t('api_keys.create') }}
          </button>
        </div>
        <p v-if="!canAdmin" style="color:var(--text-muted);margin-top:8px;">{{ $t('api_keys.admin_only') }}</p>
        <p v-if="error" style="color:var(--danger);margin-top:8px;">{{ error }}</p>
        <div v-if="onceSecret" style="margin-top:12px;padding:12px;background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);">
          <div style="font-weight:600;margin-bottom:6px;">{{ $t('api_keys.secret_once') }}</div>
          <code style="word-break:break-all;">{{ onceSecret }}</code>
        </div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>{{ $t('api_keys.name') }}</th>
              <th>{{ $t('api_keys.role') }}</th>
              <th>{{ $t('api_keys.created_by') }}</th>
              <th>{{ $t('api_keys.action') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="k in keys" :key="k.id">
              <td><code>{{ k.id }}</code></td>
              <td>{{ k.name }}</td>
              <td>{{ k.role }}</td>
              <td>{{ k.created_by }}</td>
              <td>
                <button class="btn" @click="revoke(k)" :disabled="!canAdmin">{{ $t('api_keys.revoke') }}</button>
              </td>
            </tr>
            <tr v-if="keys.length === 0">
              <td colspan="5" style="text-align:center;color:var(--text-muted);padding:40px;">{{ $t('api_keys.empty') }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  `,
  setup() {
    function $t(key, params) { return window.i18n.$t(key, params); }
    const keys = Vue.ref([]);
    const newName = Vue.ref('');
    const newRole = Vue.ref('operator');
    const creating = Vue.ref(false);
    const error = Vue.ref('');
    const onceSecret = Vue.ref('');
    const canAdmin = Vue.computed(function() { return window.store.isAdmin(); });

    async function load() {
      try {
        const data = await http.get('/api-keys');
        keys.value = Array.isArray(data) ? data : (data && data.items) || [];
      } catch (e) { error.value = e.message; }
    }
    async function createKey() {
      error.value = '';
      onceSecret.value = '';
      creating.value = true;
      try {
        const created = await http.post('/api-keys', { name: newName.value, role: newRole.value });
        onceSecret.value = (created && created.secret) || '';
        newName.value = '';
        await load();
      } catch (e) { error.value = e.message; }
      finally { creating.value = false; }
    }
    async function revoke(k) {
      error.value = '';
      try {
        await http.del('/api-keys/' + encodeURIComponent(k.id));
        await load();
      } catch (e) { error.value = e.message; }
    }
    load();
    return { $t, keys, newName, newRole, creating, error, onceSecret, canAdmin, createKey, revoke };
  },
});
