/* ============================================================
   FerroMQ Dashboard — 用户管理（admin）
   ============================================================ */
window.UsersPage = Vue.defineComponent({
  name: 'UsersPage',
  template: `
    <div>
      <div class="search-bar" style="margin-bottom:16px;">
        <div class="search-row" style="align-items:flex-end;flex-wrap:wrap;gap:8px;">
          <div class="form-group" style="margin:0;">
            <label>{{ $t('users.username') }}</label>
            <input class="form-input" v-model="newUser" />
          </div>
          <div class="form-group" style="margin:0;">
            <label>{{ $t('users.password') }}</label>
            <input class="form-input" type="password" v-model="newPass" />
          </div>
          <div class="form-group" style="margin:0;">
            <label>{{ $t('users.role') }}</label>
            <select class="form-select" v-model="newRole" style="width:140px;">
              <option value="admin">admin</option>
              <option value="operator">operator</option>
              <option value="viewer">viewer</option>
            </select>
          </div>
          <button class="btn btn-primary" @click="createUser" :disabled="!canAdmin || creating">
            {{ $t('users.create') }}
          </button>
        </div>
        <p v-if="!canAdmin" style="color:var(--text-muted);margin-top:8px;">{{ $t('users.admin_only') }}</p>
        <p v-if="error" style="color:var(--danger);margin-top:8px;">{{ error }}</p>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>{{ $t('users.username') }}</th>
              <th>{{ $t('users.role') }}</th>
              <th>{{ $t('users.enabled') }}</th>
              <th>{{ $t('users.action') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="u in users" :key="u.username">
              <td>{{ u.username }}</td>
              <td>{{ u.role }}</td>
              <td>{{ u.enabled ? $t('users.yes') : $t('users.no') }}</td>
              <td>
                <button v-if="u.enabled" class="btn" @click="disableUser(u)" :disabled="!canAdmin">
                  {{ $t('users.disable') }}
                </button>
                <button v-else class="btn btn-primary" @click="enableUser(u)" :disabled="!canAdmin">
                  {{ $t('users.enable') }}
                </button>
              </td>
            </tr>
            <tr v-if="users.length === 0">
              <td colspan="4" style="text-align:center;color:var(--text-muted);padding:40px;">{{ $t('users.empty') }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  `,
  setup() {
    function $t(key, params) { return window.i18n.$t(key, params); }
    const users = Vue.ref([]);
    const newUser = Vue.ref('');
    const newPass = Vue.ref('');
    const newRole = Vue.ref('operator');
    const creating = Vue.ref(false);
    const error = Vue.ref('');
    const canAdmin = Vue.computed(function() { return window.store.isAdmin(); });

    async function load() {
      try {
        const data = await http.get('/users');
        users.value = Array.isArray(data) ? data : (data && data.items) || [];
      } catch (e) {
        error.value = e.message;
      }
    }
    async function createUser() {
      error.value = '';
      creating.value = true;
      try {
        await http.post('/users', { username: newUser.value, password: newPass.value, role: newRole.value });
        newUser.value = '';
        newPass.value = '';
        await load();
      } catch (e) {
        error.value = e.message;
      } finally {
        creating.value = false;
      }
    }
    async function disableUser(u) {
      error.value = '';
      try {
        await http.post('/users/' + encodeURIComponent(u.username) + '/disable');
        await load();
      } catch (e) { error.value = e.message; }
    }
    async function enableUser(u) {
      error.value = '';
      try {
        await http.post('/users/' + encodeURIComponent(u.username) + '/enable');
        await load();
      } catch (e) { error.value = e.message; }
    }
    load();
    return { $t, users, newUser, newPass, newRole, creating, error, canAdmin, createUser, disableUser, enableUser };
  },
});
