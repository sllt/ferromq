/* ============================================================
   FerroMQ Dashboard — 登录页（国际化）
   P3a: 用户名/密码会话登录；可选 Bearer Token 作为 operator 回退
   ============================================================ */
window.LoginPage = Vue.defineComponent({
  name: 'LoginPage',
  template: `
    <div class="login-page">
      <div class="login-card">
        <h1>FerroMQ</h1>
        <p>{{ $t('login.subtitle') }}</p>
        <div class="form-group">
          <label>{{ $t('login.username') }}</label>
          <input class="form-input" type="text" v-model="username"
                 :placeholder="$t('login.username_placeholder')" @keyup.enter="login" />
        </div>
        <div class="form-group">
          <label>{{ $t('login.password') }}</label>
          <div class="password-wrapper">
            <input class="form-input" :type="showPassword ? 'text' : 'password'" v-model="password"
                   :placeholder="$t('login.password_placeholder')" @keyup.enter="login" />
            <button class="password-toggle" type="button" @click="showPassword = !showPassword"
                    :title="showPassword ? $t('login.hide_token') : $t('login.show_token')">
              <span v-text="showPassword ? '🙈' : '👁'"></span>
            </button>
          </div>
        </div>
        <div class="form-group">
          <button type="button" class="btn" style="background:transparent;border:none;color:var(--text-muted);padding:0;"
                  @click="showToken = !showToken">{{ $t('login.use_token') }}</button>
        </div>
        <div class="form-group" v-if="showToken">
          <label>{{ $t('login.token_optional') }}</label>
          <input class="form-input" type="password" v-model="token"
                 :placeholder="$t('login.token_placeholder')" @keyup.enter="login" />
        </div>
        <div class="form-group" v-if="error" style="color:var(--red);font-size:13px;">
          {{ error }}
        </div>
        <button class="btn btn-primary" @click="login" :disabled="loading">
          {{ loading ? $t('login.verifying') : $t('login.submit') }}
        </button>
      </div>
    </div>
  `,
  setup() {
    const username = Vue.ref('admin');
    const password = Vue.ref('');
    const token = Vue.ref('');
    const showPassword = Vue.ref(false);
    const showToken = Vue.ref(false);
    const loading = Vue.ref(false);
    const error = Vue.ref('');

    function $t(key, params) { return window.i18n.$t(key, params); }

    async function login() {
      loading.value = true;
      error.value = '';
      try {
        if (token.value.trim()) {
          store.setToken(token.value.trim());
          const me = await http.get('/auth/me');
          if (me) {
            store.setSession(me);
            location.hash = '#/';
            return;
          }
          throw new Error('Unauthorized');
        }
        if (!username.value.trim() || !password.value) {
          error.value = $t('login.error_empty');
          return;
        }
        const me = await http.post('/auth/login', {
          username: username.value.trim(),
          password: password.value,
        });
        if (me) {
          store.clearToken();
          store.setSession(me);
          location.hash = '#/';
        } else {
          throw new Error('Unauthorized');
        }
      } catch (e) {
        store.clearToken();
        store.clearSession();
        error.value = $t('login.error_invalid');
      } finally {
        loading.value = false;
      }
    }

    return { username, password, token, showPassword, showToken, loading, error, login };
  },
});
