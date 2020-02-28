/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import createAuth0Client from '@auth0/auth0-spa-js';
import Auth0Client from '@auth0/auth0-spa-js/dist/typings/Auth0Client';
import React, { useContext, useEffect, useState } from 'react';

export interface Auth0RedirectState {
  targetUrl?: string;
}

export interface Auth0User extends Omit<IdToken, '__raw'> {}

export interface Auth0ContextType {
  user?: Auth0User;
  isAuthenticated: boolean;
  isInitializing: boolean;
  isPopupOpen: boolean;
  loginWithPopup(o?: PopupLoginOptions): Promise<void>;
  handleRedirectCallback(): Promise<RedirectLoginResult>;
  getIdTokenClaims(o?: getIdTokenClaimsOptions): Promise<IdToken>;
  loginWithRedirect(o?: RedirectLoginOptions): Promise<void>;
  getTokenSilently(o?: GetTokenSilentlyOptions): Promise<string | undefined>;
  getTokenWithPopup(o?: GetTokenWithPopupOptions): Promise<string | undefined>;
  logout(o?: LogoutOptions): void;
}
interface Auth0ProviderOptions {
  children: React.ReactElement;
  onRedirectCallback(result: RedirectLoginResult): void;
}
// User permissions are added via a custom Auth0 rule
// https://community.auth0.com/t/how-do-i-add-user-permissions-to-id-token/28611
// `${AUTH0_NAMESPACE}user_authorization` where AUTH0_NAMESPACE is 'https://<domain>/'
export const AUTH0_PERMISSIONS_KEY = '${AUTH0_NAMESPACE}user_authorization';
export interface IdTokenWithPermissions extends IdToken {
  [AUTH0_PERMISSIONS_KEY]: {
    permissions: string;
  };
}

export const Auth0Context = React.createContext<Auth0ContextType | null>(null);
export const useAuth0 = () => useContext(Auth0Context)!;
export const Auth0Provider = ({
  children,
  onRedirectCallback,
  ...initOptions
}: Auth0ProviderOptions & Auth0ClientOptions) => {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isInitializing, setIsInitializing] = useState(true);
  const [isPopupOpen, setIsPopupOpen] = useState(false);
  const [user, setUser] = useState<Auth0User>();
  const [auth0Client, setAuth0Client] = useState<Auth0Client>();

  useEffect(() => {
    const initAuth0 = async () => {
      const auth0FromHook = await createAuth0Client(initOptions);
      setAuth0Client(auth0FromHook);

      if (window.location.search.includes('code=')) {
        let appState: RedirectLoginResult = {};
        try {
          ({ appState } = await auth0FromHook.handleRedirectCallback());
        } finally {
          onRedirectCallback(appState);
        }
      }

      const authed = await auth0FromHook.isAuthenticated();

      if (authed) {
        const userProfile = await auth0FromHook.getUser();

        setIsAuthenticated(true);
        setUser(userProfile);
      }

      setIsInitializing(false);
    };

    initAuth0();
  }, []);

  const loginWithPopup = async (options?: PopupLoginOptions) => {
    setIsPopupOpen(true);

    try {
      await auth0Client!.loginWithPopup(options);
    } catch (error) {
      console.error(error);
    } finally {
      setIsPopupOpen(false);
    }

    const userProfile = await auth0Client!.getUser();
    setUser(userProfile);
    setIsAuthenticated(true);
  };

  const handleRedirectCallback = async () => {
    setIsInitializing(true);

    const result = await auth0Client!.handleRedirectCallback();
    const userProfile = await auth0Client!.getUser();

    setIsInitializing(false);
    setIsAuthenticated(true);
    setUser(userProfile);

    return result;
  };

  const loginWithRedirect = (options?: RedirectLoginOptions) =>
    auth0Client!.loginWithRedirect(options);

  const getTokenSilently = (options?: GetTokenSilentlyOptions) =>
    auth0Client!.getTokenSilently(options);

  const logout = (options?: LogoutOptions) => auth0Client!.logout(options);

  const getIdTokenClaims = (options?: getIdTokenClaimsOptions) =>
    auth0Client!.getIdTokenClaims(options);

  const getTokenWithPopup = (options?: GetTokenWithPopupOptions) =>
    auth0Client!.getTokenWithPopup(options);

  return (
    <Auth0Context.Provider
      value={{
        user,
        isAuthenticated,
        isInitializing,
        isPopupOpen,
        loginWithPopup,
        loginWithRedirect,
        logout,
        getTokenSilently,
        handleRedirectCallback,
        getIdTokenClaims,
        getTokenWithPopup,
      }}
    >
      {children}
    </Auth0Context.Provider>
  );
};
